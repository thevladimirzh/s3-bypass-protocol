package transport

import (
	"context"
	"sync"
	"testing"
	"time"
)

// blockingAckStore accepts the hello and then wedges the ACK PUT, which is
// what a store answering 503 does — the write is 32 bytes and it still fails.
type blockingAckStore struct {
	*fakeStore
	release chan struct{}
	once    sync.Once
}

func (s *blockingAckStore) Upload(ctx context.Context, path string, data []byte) error {
	if path == DefaultSessionsDir+"/wedged/"+AckFile {
		<-s.releaseOnce()
	}
	return s.fakeStore.Upload(ctx, path, data)
}

func (s *blockingAckStore) releaseOnce() chan struct{} {
	s.once.Do(func() { close(s.release) })
	return s.release
}

func newBlockingAckStore() *blockingAckStore {
	return &blockingAckStore{fakeStore: newFakeStore(), release: make(chan struct{})}
}

// acceptSession ran on the same goroutine as the poll ticker and the webhook
// channel, and it performs network I/O to do it: a GET, a DELETE, a key
// exchange, and a PUT that uploadRetrying will keep retrying without bound.
//
// So one store that rejects a 32-byte PUT stopped the server noticing a new
// session for every user, indefinitely. Webhook notifications piled into a
// 64-slot channel that drops silently on overflow, and the log showed a few
// retry lines and then nothing at all.
func TestOneSlowHandshakeDoesNotBlockSessionDiscovery(t *testing.T) {
	store := newBlockingAckStore()
	defer store.releaseOnce()

	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// The wedged client arrives first and its ACK never lands.
	plant(store.fakeStore, DefaultSessionsDir+"/wedged/"+HelloFile, helloFor(t, "wedged"))
	time.Sleep(200 * time.Millisecond)

	// A second client arrives while the first handshake is still stuck.
	plant(store.fakeStore, DefaultSessionsDir+"/fresh/"+HelloFile, helloFor(t, "fresh"))

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
	store := newFakeStore()
	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	plant(store, DefaultSessionsDir+"/once/"+HelloFile, helloFor(t, "once"))

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
	case dup := <-listener.incoming:
		t.Fatalf("session delivered twice to the incoming channel: %v", dup != nil)
	case <-time.After(300 * time.Millisecond):
	}
}

// helloFor builds a hello the server will actually accept: the session id
// followed by a real X25519 public key. An all-zero key is a low-order point
// and key derivation rejects it, which made an earlier version of the
// double-accept spec fail with "never accepted" for reasons that had nothing
// to do with double acceptance.
func helloFor(t *testing.T, id string) []byte {
	t.Helper()
	_, pub, err := GenerateX25519()
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(id), pub...)
}
