package transport

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// errPutFlaky is the failure a backend returns under load. The live incident
// showed exactly this shape: a file the writer could not deliver.
var errPutFlaky = errors.New("put failed: backend unavailable")

// flakyStore fails the first `failFirst` Upload attempts of every path, then
// accepts. It models a saturated S3 that drops a few requests — the condition
// that produced the permanently missing read-direction object in the field
// (hole at seq 12 with 120 later files present). Any storage.Storage can be
// wrapped, so the same fault can be injected over the fake store or over the
// real local-filesystem backend the end-to-end stand uses.
type flakyStore struct {
	storage.Storage
	mu        sync.Mutex
	failFirst int
	attempts  map[string]int
}

func newFlakyStore(failFirst int) *flakyStore {
	return &flakyStore{Storage: newFakeStore(), failFirst: failFirst, attempts: map[string]int{}}
}

func (f *flakyStore) Upload(ctx context.Context, path string, data []byte) error {
	f.mu.Lock()
	n := f.attempts[path]
	f.attempts[path] = n + 1
	f.mu.Unlock()
	if n < f.failFirst {
		return errPutFlaky
	}
	return f.Storage.Upload(ctx, path, data)
}

func (f *flakyStore) attemptCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[path]
}

// --- B-19: a file that fails must be retried until it lands -----------------

// Root cause of the load stall: uploadWorker gave a file exactly
// `uploadAttempts` tries and then dropped it. The peer reads strictly in
// order, so one dropped file is a permanent hole — it can never be skipped,
// so the session wedges until the watchdog tears it down. A writer must
// either deliver the file or die trying; "give up and continue" must not be
// an option.
func TestFailedUploadIsRetriedUntilDelivered(t *testing.T) {
	const failFirst = 3 // exactly uploadAttempts: upstream dropped the file here
	store := newFlakyStore(failFirst)
	c := NewConn(ConnConfig{Store: store, SessionID: GenerateSessionID()})
	defer c.Close()

	path := "sessions/retry/w_00000000"
	payload := []byte("must survive a flaky backend")

	if err := c.uploadUntilDelivered(path, payload); err != nil {
		t.Fatalf("a file the backend rejected must not be reported as delivered: %v", err)
	}

	if got := store.attemptCount(path); got < failFirst+1 {
		t.Fatalf("upload gave up after %d attempts (backend rejected the first %d); the file was dropped, leaving a hole",
			got, failFirst)
	}
	got, err := store.Download(context.Background(), path)
	if err != nil {
		t.Fatalf("the file must actually be readable by the peer: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("delivered bytes = %q, want %q", got, payload)
	}
}

// The retry must stop when the session is gone — a writer that retries
// forever on a closed session would leak a goroutine and a bucket object.
func TestUploadRetryStopsWhenSessionCloses(t *testing.T) {
	store := newFlakyStore(1 << 30) // never succeeds
	c := NewConn(ConnConfig{Store: store, SessionID: GenerateSessionID()})

	done := make(chan error, 1)
	go func() {
		done <- c.uploadUntilDelivered("sessions/retry/w_00000000", []byte("x"))
	}()
	time.Sleep(150 * time.Millisecond)
	c.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("a permanently failing backend must not report success")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("upload retry ignored the closed session and kept running")
	}
}

// Retries must be spaced, not a hot loop: hammering a backend that is
// already overloaded makes the outage worse.
func TestUploadRetryBacksOff(t *testing.T) {
	store := newFlakyStore(1 << 30)
	c := NewConn(ConnConfig{Store: store, SessionID: GenerateSessionID()})
	defer c.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.uploadUntilDelivered("sessions/retry/w_00000000", []byte("x"))
	}()
	time.Sleep(600 * time.Millisecond)
	c.Close()
	<-done

	attempts := store.attemptCount("sessions/retry/w_00000000")
	if attempts == 0 {
		t.Fatalf("expected at least one attempt")
	}
	// 600ms of retries at a ~100ms floor is a handful; a hot loop would be
	// hundreds. The bound is deliberately loose — this pins "not a spin", not
	// an exact cadence.
	if attempts > 12 {
		t.Fatalf("upload retried %d times in ~600ms — the retry loop is not backing off", attempts)
	}
}

// Every retry has to be visible. The whole incident was undiagnosable until
// the core said "upload failed"; a silent retry is just a slower stall.
func TestUploadRetryIsLogged(t *testing.T) {
	store := newFlakyStore(2)
	c := NewConn(ConnConfig{Store: store, SessionID: GenerateSessionID()})
	defer c.Close()

	var out strings.Builder
	restore := captureLog(&out)
	err := c.uploadUntilDelivered("sessions/retry/w_00000000", []byte("x"))
	restore()

	if err != nil {
		t.Fatalf("upload should have succeeded on retry: %v", err)
	}
	if !strings.Contains(out.String(), "upload retry") {
		t.Fatalf("retries must be logged, got: %s", out.String())
	}
}

// --- The failure this exists to prevent, end to end ------------------------

// With a flaky backend the writer must still deliver every byte in order: no
// dropped chunk, no wedge. This is the property whose absence produced
// "hole at seq N ... (120 present)".
func TestFlakyBackendStillDeliversEveryByteInOrder(t *testing.T) {
	// More failures than uploadAttempts — upstream would drop the file here and
	// wedge the reader forever.
	store := newFlakyStore(5)
	// Both ends of a fedarisha session share one directory in the bucket: each
	// side writes its own prefix and reads the other's.
	sessDir := "sessions/flaky"

	// The client writes PrefixClient and the server reads it, so the two ends are
	// configured through the constructor rather than by mutating a prefix after
	// the poll loop has already started reading it.
	writer := NewConn(ConnConfig{Store: store, SessionDir: sessDir, SessionID: GenerateSessionID(), IsClient: true})
	defer writer.Close()
	reader := NewConn(ConnConfig{Store: store, SessionDir: sessDir, SessionID: GenerateSessionID()})
	defer reader.Close()

	payload := strings.Repeat("fed-arisha-payload-", 4000) // ~72 KB
	go func() { _, _ = writer.Write([]byte(payload)) }()

	buf := make([]byte, len(payload)+16)
	var got atomic.Int64
	done := make(chan error, 1)
	go func() {
		for int(got.Load()) < len(payload) {
			n, err := reader.Read(buf[got.Load():])
			got.Add(int64(n))
			if err != nil {
				done <- fmt.Errorf("read failed after %d bytes: %w", got.Load(), err)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("reader wedged after %d of %d bytes — a dropped file, the exact stall we are fixing", got.Load(), len(payload))
	}
	if string(buf[:got.Load()]) != payload {
		t.Fatalf("stream corrupted: got %d bytes, content differs from what was written", got.Load())
	}
}
