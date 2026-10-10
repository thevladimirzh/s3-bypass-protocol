package transport

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage/local"
)

// blockingAckStore accepts the hello and then wedges the ACK PUT, which is
// what a store answering 503 does — the write is 32 bytes and it still fails.
//
// It wraps the local backend rather than a fake because the listener discovers
// sessions by listing directories, and the in-memory fake lists flat paths. A
// test that fakes the wrong shape verifies nothing about discovery.
type blockingAckStore struct {
	*local.Local
	release chan struct{}
	once    sync.Once
}

func (s *blockingAckStore) Upload(ctx context.Context, path string, data []byte) error {
	if path == DefaultSessionsDir+"/wedged/"+AckFile {
		// Wait for the test to let go. releaseNow is deliberately not called
		// from here: a helper that both closes the channel and returns it
		// releases the very first caller, which is how an earlier version of
		// this spec passed against the code it was written to catch.
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Local.Upload(ctx, path, data)
}

func (s *blockingAckStore) releaseNow() {
	s.once.Do(func() { close(s.release) })
}

func newBlockingAckStore(t *testing.T) (*blockingAckStore, string) {
	t.Helper()
	root := t.TempDir()
	return &blockingAckStore{
		Local:   local.New(local.Config{RootDir: root}),
		release: make(chan struct{}),
	}, root
}

// helloFor builds a hello the server will actually accept: the session id
// followed by a real X25519 public key. An all-zero key is a low-order point
// and key derivation rejects it, which made an earlier version fail with
// "never accepted" for reasons that had nothing to do with what it tested.
func helloFor(t *testing.T, id string) []byte {
	t.Helper()
	_, pub, err := GenerateX25519()
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(id), pub...)
}

func plantHelloOnDisk(t *testing.T, root, sessionsDir, id string) {
	t.Helper()
	dir := filepath.Join(root, sessionsDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, HelloFile), helloFor(t, id), 0o644); err != nil {
		t.Fatal(err)
	}
}

// acceptSession ran on the same goroutine as the poll ticker and the webhook
// channel, and it performs network I/O to do it: a GET, a DELETE, a key
// exchange, and a PUT that uploadRetrying retries without bound and without a
// per-attempt timeout.
//
// So one store that rejects a 32-byte PUT stopped the server noticing a new
// session for every user, indefinitely. Webhook notifications piled into a
// 64-slot channel that drops silently on overflow, and the log showed a few
// retry lines and then nothing at all — which is what a wedged server looks
// like from the outside.
func TestOneSlowHandshakeDoesNotBlockSessionDiscovery(t *testing.T) {
	store, root := newBlockingAckStore(t)
	defer store.releaseNow()

	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// The wedged client arrives first and its ACK never lands.
	plantHelloOnDisk(t, root, DefaultSessionsDir, "wedged")
	time.Sleep(300 * time.Millisecond)

	// A second client arrives while the first handshake is still stuck.
	plantHelloOnDisk(t, root, DefaultSessionsDir, "fresh")

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-listener.incoming:
			return // discovery kept working while the other handshake was stuck
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a second session was never discovered while an earlier handshake was stuck. " +
		"Discovery shares a goroutine with the handshake, so one wedged ACK stops " +
		"every user from connecting.")
}

// Moving handshakes off the ticker must not let the same session directory be
// accepted twice, which the previous serialisation happened to guarantee.
func TestConcurrentScansDoNotDoubleAcceptASession(t *testing.T) {
	store, root := newBlockingAckStore(t)
	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	plantHelloOnDisk(t, root, DefaultSessionsDir, "once")

	// Same directory, many concurrent accepts — as a ticker and a webhook
	// would produce once handshakes are no longer serialised.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			listener.acceptSession(DefaultSessionsDir + "/once")
		}()
	}
	wg.Wait()

	select {
	case <-listener.incoming:
	case <-time.After(2 * time.Second):
		t.Fatal("session was never accepted")
	}

	select {
	case <-listener.incoming:
		t.Fatal("session delivered twice to the incoming channel")
	case <-time.After(300 * time.Millisecond):
	}
}
