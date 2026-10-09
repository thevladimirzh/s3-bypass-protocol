package transport

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

const x25519KeySize = 32

// ackRetrySchedule is how long to wait for the server's ACK in each successive
// attempt: short first, growing later, so the common case (a peer that answers
// within a second or two) is fast, the slow case still gets patience, and the
// total stays below the old flat 60s single wait.
var ackRetrySchedule = func() []time.Duration {
	return []time.Duration{
		5 * time.Second,
		10 * time.Second,
		20 * time.Second,
		30 * time.Second,
	}
}

func ackRetryBudget() time.Duration {
	var total time.Duration
	for _, d := range ackRetrySchedule() {
		total += d
	}
	return total
}

// waitForAck polls the ACK file for up to window. It returns the (possibly empty)
// contents, or an error only if the caller's context was cancelled — a missing or
// not-yet-complete ACK is a normal "keep waiting" outcome.
func (d *Dialer) waitForAck(ctx context.Context, path string, window time.Duration) ([]byte, error) {
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		data, err := d.Store.Download(ctx, path)
		if err == nil && len(data) >= x25519KeySize {
			return data, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil, nil
}

// Dialer creates outbound FEDARISHA connections (client side).
type Dialer struct {
	Store         storage.Storage
	SessionsDir   string // e.g. "sessions"
	PollInterval  time.Duration
	WriteInterval time.Duration
	IdleTimeout   time.Duration
	MaxFileSize   int
}

// Dial creates a new session and returns a net.Conn backed by cloud storage.
// Performs X25519 key exchange with the server to establish E2E encryption.
func (d *Dialer) Dial(ctx context.Context) (*Conn, error) {
	sessID := GenerateSessionID()
	sessDir := effectiveSessionsDir(d.SessionsDir) + "/" + sessID

	log.Printf("[fedarisha-client] dialing new session %s", sessID[:8])

	// Generate client X25519 key pair.
	privKey, pubKey, err := GenerateX25519()
	if err != nil {
		return nil, fmt.Errorf("fedarisha dial: %w", err)
	}

	// Create session directory.
	if err := d.Store.EnsureDir(ctx, sessDir); err != nil {
		return nil, fmt.Errorf("fedarisha dial: create session dir: %w", err)
	}

	// Write hello file: sessID + client public key.
	helloData := make([]byte, len(sessID)+x25519KeySize)
	copy(helloData, sessID)
	copy(helloData[len(sessID):], pubKey)

	helloPath := sessDir + "/" + HelloFile
	if err := d.Store.Upload(ctx, helloPath, helloData); err != nil {
		return nil, fmt.Errorf("fedarisha dial: write hello: %w", err)
	}

	// Wait for server ACK containing server's public key.
	//
	// The wait is a backoff schedule, not a flat 60s: a flat wait meant every
	// failed handshake cost a full minute of dead time before the outbound could
	// try again, so a load-induced teardown took minutes to recover (beta
	// observation 2026-10-09: three consecutive dials exhausted ~3 of the ~4
	// minutes of outage). The first window is short so a wedged peer fails over
	// promptly, while the last window is the longest so a genuinely slow peer
	// still gets a real chance.
	ackPath := sessDir + "/" + AckFile
	var serverPub []byte
	for _, window := range ackRetrySchedule() {
		data, err := d.waitForAck(ctx, ackPath, window)
		if err != nil {
			_ = d.Store.Delete(ctx, helloPath)
			_ = d.Store.Delete(ctx, sessDir)
			return nil, err
		}
		if len(data) >= x25519KeySize {
			serverPub = data[:x25519KeySize]
			log.Printf("[fedarisha-client] session %s accepted by server (encrypted)", sessID[:8])
			_ = d.Store.Delete(ctx, ackPath)
			break
		}
		log.Printf("[fedarisha-client] no ACK for session %s after %v, waiting longer", sessID[:8], window)
	}
	if serverPub == nil {
		_ = d.Store.Delete(ctx, helloPath)
		_ = d.Store.Delete(ctx, sessDir)
		return nil, fmt.Errorf("fedarisha dial: server did not ACK within %v", ackRetryBudget())
	}

	// Derive shared AES-256-GCM cipher.
	aead, err := DeriveAEAD(privKey, serverPub, sessID)
	if err != nil {
		return nil, fmt.Errorf("fedarisha dial: derive key: %w", err)
	}

	return NewConn(ConnConfig{
		Store:         d.Store,
		SessionID:     sessID,
		SessionDir:    sessDir,
		IsClient:      true,
		PollInterval:  d.PollInterval,
		WriteInterval: d.WriteInterval,
		IdleTimeout:   d.IdleTimeout,
		MaxFileSize:   d.MaxFileSize,
		Cipher:        aead,
	}), nil
}
