package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// stallingStore accepts uploads that never complete, so the upload workers
// stay pinned inside their retry loop exactly as they do when the object store
// starts answering 503.
type stallingStore struct {
	*fakeStore
	blocked chan struct{} // closed to release the pinned uploads
	entered sync.WaitGroup
	started chan struct{}
	once    sync.Once
}

func (s *stallingStore) Upload(ctx context.Context, path string, data []byte) error {
	s.entered.Add(1)
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.blocked:
		return errors.New("released")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newStallingStore() *stallingStore {
	return &stallingStore{
		fakeStore: newFakeStore(),
		blocked:   make(chan struct{}),
		started:   make(chan struct{}),
	}
}

func newCloseTestConn(t *testing.T, store *stallingStore) *Conn {
	t.Helper()
	conn := NewConn(ConnConfig{
		Store:      store,
		SessionID:  "0123456789abcdef0123456789abcdef",
		SessionDir: "sessions/0123456789abcdef0123456789abcdef",
		IsClient:   true,
	})
	t.Cleanup(func() {
		select {
		case <-store.blocked:
		default:
			close(store.blocked)
		}
	})
	return conn
}

// Close() flushed before it published the signals that flush itself waits on:
//
//	c.flush()          // 412 — parks here on a full uploadQueue
//	close(c.closed)    // 413
//	c.cancel()         // 414
//	close(c.uploadQueue)
//
// sendChunks selects on uploadQueue, closed and ctx.Done together, so with all
// three unavailable it never wakes. That deadlock holds closeOnce, so every
// other Close() caller blocks behind it too, and the cleanup goroutine below
// line 417 never starts — the session's objects stay in the bucket and the
// outbound never re-dials.
func TestCloseDoesNotDeadlockOnFullUploadQueue(t *testing.T) {
	store := newStallingStore()
	conn := newCloseTestConn(t, store)

	// Saturate: every worker pinned inside Upload, the queue full behind them.
	// 64 matches the constructor's buffer at conn.go:314.
	for i := 0; i < uploadWorkers+64; i++ {
		conn.sendChunks([]pendingChunk{{data: make([]byte, 16), seq: uint64(i)}})
	}
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no upload reached the store")
	}
	time.Sleep(50 * time.Millisecond)

	// A small Write stays in writeBuf — below maxFileSize, so no forceFlush.
	// This is what makes Close() have something to flush, and therefore
	// something to block on. Without it the deadlock hides.
	if _, err := conn.Write(make([]byte, 32)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.Close()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return: it deadlocks on a full upload queue")
	}
	close(store.blocked)
}

// Closing the queue that sendChunks may be sending on is a second face of the
// same ordering problem: a send on a closed channel inside a select is ready,
// so it panics rather than failing.
func TestConcurrentWriteDuringCloseDoesNotPanic(t *testing.T) {
	store := newStallingStore()
	conn := newCloseTestConn(t, store)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Producers racing the shutdown, which is the ordinary case: yamux's
	// send goroutine is inside Write while the peer goes away.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				conn.Write(make([]byte, 512))
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	conn.Close()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(store.blocked)
}

// A second Close must not be able to wedge behind the first.
func TestSecondCloseDoesNotBlockOnFirst(t *testing.T) {
	store := newStallingStore()
	conn := newCloseTestConn(t, store)

	for i := 0; i < uploadWorkers+70; i++ {
		conn.sendChunks([]pendingChunk{{data: make([]byte, 16), seq: uint64(i)}})
	}
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no upload reached the store")
	}
	time.Sleep(50 * time.Millisecond)

	first := make(chan struct{})
	go func() { defer close(first); conn.Close() }()

	second := make(chan struct{})
	go func() { defer close(second); conn.Close() }()

	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("second Close() blocked behind the first")
	}
	close(store.blocked)
	<-first
}
