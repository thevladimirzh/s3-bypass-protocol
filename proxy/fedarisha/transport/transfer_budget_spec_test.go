package transport

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// slowLinkStore uploads at a fixed rate, the shape of a congested mobile
// uplink — which is the population this tunnel exists for.
type slowLinkStore struct {
	storage.Storage
	perByte time.Duration
	mu      sync.Mutex
	aborted int
	bytes   int
}

func (s *slowLinkStore) Upload(ctx context.Context, path string, data []byte) error {
	select {
	case <-time.After(time.Duration(len(data)) * s.perByte):
		s.mu.Lock()
		s.bytes += len(data)
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		s.aborted++
		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *slowLinkStore) counts() (delivered, aborted int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes, s.aborted
}

// uploadTimeout capped the whole PUT, body included, at a flat eight seconds.
// A 2MB file therefore needed better than 256 KB/s to have any chance, and on
// a marginal link it did not get one: every attempt was aborted mid-body, the
// partial upload discarded, and the retry began with a fresh budget. That is a
// livelock rather than a retry — no amount of waiting helps a file that always
// needs longer than the budget allows.
//
// It was worst where it hurt most. Hedging every 3 seconds with all three
// attempts dying at the 8s mark means a file that never lands costs 6MB of
// uplink and delivers nothing, on exactly the link that can least afford it.
func TestLargeFileDeliversOnASlowLink(t *testing.T) {
	// Shrink the budget so the test need not spend ten seconds proving a
	// payload can outlast it.
	shrinkUploadTimeout(t, 300*time.Millisecond)

	store := &slowLinkStore{
		Storage: newFakeStore(),
		perByte: 2 * time.Microsecond,
	}

	const size = 512 * 1024
	conn := NewConn(ConnConfig{
		Store:       store,
		SessionID:   GenerateSessionID(),
		SessionDir:  "sessions/slow",
		MaxFileSize: size,
	})
	defer conn.Close()

	// Incompressible payload. An all-zero buffer of the same size compresses
	// to roughly five hundred bytes before it reaches Upload, which made an
	// earlier version of this test wait for a transfer that was never going to
	// be slow.
	payload := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(payload)

	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Wait for the payload itself to land, not merely for some upload to
	// succeed: the connection emits small files too, and an earlier version of
	// this test passed against broken code by counting one of those.
	deadline := time.Now().Add(20 * time.Second)
	for {
		delivered, aborted := store.counts()
		if delivered >= size {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("payload never landed after %d bytes and %d aborted attempts. "+
				"A file that needs longer than the budget can never be delivered, "+
				"however many times it is retried.", delivered, aborted)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
