package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage/local"
)

// The end-to-end stand runs BOTH ends of the protocol in one process against
// the local filesystem backend, so stalls can be reproduced and fixed with no
// VPS, bucket or credentials. This is what the field incident looked like:
// a parallel-download burst through a real fedarisha session.
//
// Before it existed, the read-path stall could only be reproduced against the
// owner's live tunnel, where each run cost a manual VPN toggle.

const (
	standPoll   = 20 * time.Millisecond
	standWrite  = 20 * time.Millisecond
	standIdle   = 30 * time.Second
	standAccept = 10 * time.Second
)

// standPair wires a Listener (server) and a Dialer (client) over one local
// storage root and returns the two ends of a freshly negotiated session.
func standPair(t *testing.T, ctx context.Context) (client, server *Conn, cleanup func()) {
	t.Helper()

	store := local.New(local.Config{RootDir: t.TempDir()})
	if err := store.Init(ctx); err != nil {
		t.Fatalf("init local store: %v", err)
	}

	ln, err := Listen(ctx, store, DefaultSessionsDir, ListenOpts{
		PollInterval:  standPoll,
		WriteInterval: standWrite,
		IdleTimeout:   standIdle,
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	dialer := &Dialer{
		Store:         store,
		SessionsDir:   DefaultSessionsDir,
		PollInterval:  standPoll,
		WriteInterval: standWrite,
		IdleTimeout:   standIdle,
	}

	type dialResult struct {
		conn *Conn
		err  error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		c, err := dialer.Dial(ctx)
		dialed <- dialResult{c, err}
	}()

	var clientConn *Conn
	select {
	case res := <-dialed:
		if res.err != nil {
			t.Fatalf("client dial: %v", res.err)
		}
		clientConn = res.conn
	case <-time.After(standAccept):
		t.Fatalf("client never completed the handshake")
	}

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	var serverConn *Conn
	select {
	case nc := <-accepted:
		if nc == nil {
			t.Fatalf("listener stopped before accepting")
		}
		serverConn = nc.(*Conn)
	case <-time.After(standAccept):
		t.Fatalf("server never accepted the session")
	}

	return clientConn, serverConn, func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
		_ = ln.Close()
	}
}

func randomPayload(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

// drain reads until want bytes have arrived or the deadline expires.
func drain(t *testing.T, c *Conn, want int, timeout time.Duration) []byte {
	t.Helper()
	buf := make([]byte, want+1024)
	got := 0
	deadline := time.Now().Add(timeout)
	for got < want && time.Now().Before(deadline) {
		c.SetReadDeadline(deadline)
		n, err := c.Read(buf[got:])
		got += n
		if err != nil {
			break
		}
	}
	return buf[:got]
}

// A session negotiates, carries a payload in both directions and closes.
func TestStandSessionCarriesPayloadBothWays(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, server, cleanup := standPair(t, ctx)
	defer cleanup()

	toServer := randomPayload(64 * 1024)
	toClient := randomPayload(48 * 1024)

	go func() { _, _ = client.Write(toServer) }()
	go func() { _, _ = server.Write(toClient) }()

	gotServer := drain(t, server, len(toServer), standAccept)
	if !bytes.Equal(gotServer, toServer) {
		t.Fatalf("server received %d bytes, want %d (content mismatch)", len(gotServer), len(toServer))
	}
	gotClient := drain(t, client, len(toClient), standAccept)
	if !bytes.Equal(gotClient, toClient) {
		t.Fatalf("client received %d bytes, want %d (content mismatch)", len(gotClient), len(toClient))
	}
}

// The incident: several parallel streams through independent sessions. Every
// stream must arrive complete and in order, and no session may wedge.
func TestStandSurvivesParallelStreams(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const streams = 4
	const size = 128 * 1024

	payloads := make([][]byte, streams)
	type wire struct {
		client, server *Conn
		cleanup        func()
		want           []byte
		got            []byte
	}
	wires := make([]*wire, streams)
	for i := 0; i < streams; i++ {
		payloads[i] = randomPayload(size)
		c, s, cl := standPair(t, ctx)
		wires[i] = &wire{client: c, server: s, cleanup: cl, want: payloads[i]}
	}

	var wg sync.WaitGroup
	for i, w := range wires {
		wg.Add(1)
		go func(i int, w *wire) {
			defer wg.Done()
			go func() { _, _ = w.client.Write(w.want) }()
			w.got = drain(t, w.server, len(w.want), 30*time.Second)
		}(i, w)
	}
	wg.Wait()

	for i, w := range wires {
		w.cleanup()
		if len(w.got) != len(w.want) {
			t.Fatalf("stream %d: received %d of %d bytes — a stalled or dropped file", i, len(w.got), len(w.want))
		}
		if !bytes.Equal(w.got, w.want) {
			t.Fatalf("stream %d: payload corrupted in transit", i)
		}
	}
}

// One large stream: the shape that produced "hole at seq N ... (M present)".
// The writer flushes continuously and the reader must never lose ordering.
func TestStandDeliversLargeStreamWithoutHoles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, server, cleanup := standPair(t, ctx)
	defer cleanup()

	payload := randomPayload(2 * 1024 * 1024) // ~2 MB, spans many chunk files
	go func() { _, _ = client.Write(payload) }()

	got := drain(t, server, len(payload), 60*time.Second)
	if len(got) != len(payload) {
		t.Fatalf("large stream delivered %d of %d bytes", len(got), len(payload))
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("large stream corrupted")
	}
}

// A backend that intermittently rejects writes must not cost a single byte:
// this is the delivery guarantee (B-19) proven through a real session pair
// rather than through a unit-level fake.
func TestStandSurvivesFlakyBackend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := local.New(local.Config{RootDir: t.TempDir()})
	if err := store.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	flaky := &flakyStore{Storage: store, failFirst: 4, attempts: map[string]int{}}

	ln, err := Listen(ctx, flaky, DefaultSessionsDir, ListenOpts{
		PollInterval: standPoll, WriteInterval: standWrite, IdleTimeout: standIdle,
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	dialer := &Dialer{Store: flaky, SessionsDir: DefaultSessionsDir, PollInterval: standPoll, WriteInterval: standWrite, IdleTimeout: standIdle}
	clientConn, err := dialer.Dial(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	var serverConn *Conn
	select {
	case nc := <-accepted:
		serverConn = nc.(*Conn)
	case <-time.After(standAccept):
		t.Fatalf("listener never accepted under a flaky backend")
	}
	defer serverConn.Close()

	payload := randomPayload(256 * 1024)
	go func() { _, _ = clientConn.Write(payload) }()

	got := drain(t, serverConn, len(payload), 60*time.Second)
	if len(got) != len(payload) {
		t.Fatalf("flaky backend cost us data: %d of %d bytes arrived", len(got), len(payload))
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload corrupted across a flaky backend")
	}
}
