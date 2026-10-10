package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// --- M6: teardown and housekeeping failures were silent -------------------
//
// Two separate leaks, both invisible.
//
// cleanupSession bailed on the first List error and returned. Teardown runs in
// a detached goroutine with 60s of budget, and the sessions it sweeps are
// exactly the long ones — the comment above the call says they accumulate
// hundreds to thousands of files. So one transient blip at the moment a
// connection drops leaves every one of those objects in the bucket, forever,
// with nothing logged.
//
// The per-file Delete in the read path had the same problem in a different
// shape: one detached goroutine per consumed file, unbounded, errors dropped.
// A fast download spawns them faster than the backend retires them.

// flakyListStore fails the first listCalls-failing List calls, then behaves.
type flakyListStore struct {
	storage.Storage

	mu      sync.Mutex
	failing int
	lists   int
}

func (s *flakyListStore) List(ctx context.Context, dir, prefix string) ([]storage.FileInfo, error) {
	s.mu.Lock()
	s.lists++
	if s.failing > 0 {
		s.failing--
		s.mu.Unlock()
		return nil, errors.New("SlowDown: reduce your request rate")
	}
	s.mu.Unlock()
	return s.Storage.List(ctx, dir, prefix)
}

func (s *flakyListStore) listCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists
}

// Spec: a transient List failure must not cost the whole session. Teardown is
// best-effort by design, but "best-effort" was implemented as "give up on the
// first error", which turns one blip into a permanent leak.
func TestCleanupSessionSurvivesATransientListFailure(t *testing.T) {
	inner := newFakeStore()
	const sessDir = "sessions/cleanup-flaky"
	for i := 0; i < 5; i++ {
		plant(inner, fmt.Sprintf("%s/s_%d", sessDir, i), []byte("x"))
	}

	store := &flakyListStore{Storage: inner, failing: 1}
	// Hand-built: cleanupSession touches only store and sessDir, and a real
	// Conn would start a poll loop that competes for the same fake.
	c := &Conn{store: store, sessDir: sessDir}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.cleanupSession(ctx)

	if got := store.listCount(); got < 2 {
		t.Fatalf("cleanupSession gave up after %d List call(s); a transient failure must be retried "+
			"while the teardown budget lasts, or the whole session leaks", got)
	}
	if remaining := len(inner.files); remaining != 0 {
		t.Fatalf("%d object(s) survived cleanup after a transient List failure; the session leaks", remaining)
	}
}

// Spec: the failure has to be visible. Silently dropping it is what left an
// operator with a growing bucket and no explanation.
func TestCleanupSessionReportsAFailedList(t *testing.T) {
	inner := newFakeStore()
	const sessDir = "sessions/cleanup-log"
	plant(inner, sessDir+"/s_0", []byte("x"))

	store := &flakyListStore{Storage: inner, failing: 1 << 30} // never recovers
	var out syncBuffer
	restore := captureLog(&out)
	defer restore()

	c := &Conn{store: store, sessDir: sessDir}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c.cleanupSession(ctx)

	if !out.has("SlowDown") {
		t.Fatalf("a failed List during cleanup must be logged; got: %s", out.String())
	}
}

// deleteTrackingStore counts Delete calls, records the peak in flight, and can
// be made to block or fail.
type deleteTrackingStore struct {
	storage.Storage

	mu       sync.Mutex
	inFlight int
	peak     int
	deleted  int

	block chan struct{}
	err   error
}

func (s *deleteTrackingStore) Delete(ctx context.Context, path string) error {
	s.mu.Lock()
	s.inFlight++
	s.deleted++
	if s.inFlight > s.peak {
		s.peak = s.inFlight
	}
	block, err := s.block, s.err
	s.mu.Unlock()

	if block != nil {
		select {
		case <-ctx.Done():
		case <-block:
		}
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()
	return s.Storage.Delete(ctx, path)
}

func (s *deleteTrackingStore) peakDeletes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

func (s *deleteTrackingStore) deleteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleted
}

// plantSeqs writes n read-direction files for sequences 0..n-1.
func plantSeqs(t *testing.T, c *Conn, store *fakeStore, sessDir string, n int) {
	t.Helper()
	for seq := 0; seq < n; seq++ {
		data, err := c.encodeTestFile(uint64(seq), []byte("payload"))
		if err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("%s/%s", sessDir, SeqFileName(c.readPrefix, uint64(seq)))
		if err := store.Upload(context.Background(), path, data); err != nil {
			t.Fatal(err)
		}
	}
}

// readAtLeast drains until want bytes have arrived or the budget runs out.
func readAtLeast(c *Conn, want int, budget time.Duration) int {
	deadline := time.Now().Add(budget)
	got := 0
	buf := make([]byte, 256)
	for got < want && time.Now().Before(deadline) {
		n, err := c.Read(buf)
		got += n
		if err != nil {
			return got
		}
	}
	return got
}

// Spec: the read path must not be able to flood the backend with DELETEs.
// maxReadBatch files can be consumed in a single round, and each one used to
// spawn its own goroutine with no cap — so the faster the download, the more
// requests the housekeeping path issued at the backend it is trying to keep up
// with.
func TestConsumedFilesDoNotFloodTheBackendWithDeletes(t *testing.T) {
	inner := newFakeStore()
	release := make(chan struct{})
	defer close(release)
	store := &deleteTrackingStore{Storage: inner, block: release}

	sessDir := "sessions/delete-flood"
	c := NewConn(ConnConfig{Store: store, SessionDir: sessDir, SessionID: GenerateSessionID()})
	defer c.Close()

	const files = maxReadBatch
	plantSeqs(t, c, inner, sessDir, files)

	if got := readAtLeast(c, files*len("payload"), 30*time.Second); got < files*len("payload") {
		t.Fatalf("read only %d bytes; the spec never exercised the deletes", got)
	}

	if peak := store.peakDeletes(); peak > maxDeleteConcurrency {
		t.Fatalf("%d DELETEs were in flight at once, over the cap of %d; "+
			"housekeeping must not compete with the transfer it is tidying up after", peak, maxDeleteConcurrency)
	}
	if got := store.deleteCount(); got == 0 {
		t.Fatalf("no Delete was issued at all; the cap must bound concurrency, not suppress cleanup")
	}
}

// Spec: when the queue does fill, the shed has to be reported rather than
// absorbed. Dropping is safe because cleanupSession sweeps at Close, but that
// is a design argument, and an unreported shed is indistinguishable from the
// deletion having happened. The reader must be unaffected either way — it never
// waits on housekeeping.
func TestShedDeletesAreReportedAndDoNotStallTheReader(t *testing.T) {
	inner := newFakeStore()
	release := make(chan struct{})
	defer close(release)
	store := &deleteTrackingStore{Storage: inner, block: release}

	var out syncBuffer
	restore := captureLog(&out)
	defer restore()

	sessDir := "sessions/delete-shed"
	c := NewConn(ConnConfig{Store: store, SessionDir: sessDir, SessionID: GenerateSessionID()})
	defer c.Close()

	// Enough files that several rounds are consumed while the workers stay
	// blocked, so the queue is forced past its depth.
	const files = deleteQueueDepth + maxReadBatch*2
	plantSeqs(t, c, inner, sessDir, files)

	want := files * len("payload")
	if got := readAtLeast(c, want, 120*time.Second); got != want {
		t.Fatalf("shedding deletes cost the reader its data: got %d of %d bytes", got, want)
	}

	if !waitFor(func() bool { return strings.Contains(out.String(), "delete queue full") }, 15*time.Second) {
		t.Fatalf("a shed housekeeping delete must be reported; got: %s", out.String())
	}
}

// Spec: a delete that fails is evidence, not noise to discard. Swallowed, a
// permission problem or a throttling response leaves objects behind with no
// trace, and the sweep at Close is the only thing that will ever remove them.
func TestFailedConsumedFileDeletesAreReported(t *testing.T) {
	inner := newFakeStore()
	store := &deleteTrackingStore{Storage: inner, err: errors.New("AccessDenied: DeleteObject not allowed")}

	sessDir := "sessions/delete-log"
	var out syncBuffer
	restore := captureLog(&out)
	defer restore()

	c := NewConn(ConnConfig{Store: store, SessionDir: sessDir, SessionID: GenerateSessionID()})
	defer c.Close()

	plantSeqs(t, c, inner, sessDir, 4)
	readAtLeast(c, 4*len("payload"), 30*time.Second)

	if !waitFor(func() bool { return out.has("AccessDenied") }, 10*time.Second) {
		t.Fatalf("a failed Delete must be logged; the object is left behind silently. Got: %s", out.String())
	}
}

// Spec: a failed delete must not take the read path down with it. The read
// direction and the housekeeping it schedules are independent: the bytes are
// already in hand, and losing them would be worse than the leak.
func TestFailedDeleteDoesNotDisturbDelivery(t *testing.T) {
	inner := newFakeStore()
	store := &deleteTrackingStore{Storage: inner, err: errors.New("SlowDown")}

	restore := captureLog(io.Discard)
	defer restore()

	sessDir := "sessions/delete-resilient"
	c := NewConn(ConnConfig{Store: store, SessionDir: sessDir, SessionID: GenerateSessionID()})
	defer c.Close()

	const files = 8
	plantSeqs(t, c, inner, sessDir, files)
	want := files * len("payload")
	if got := readAtLeast(c, want, 30*time.Second); got != want {
		t.Fatalf("a failing Delete cost us the read: got %d of %d bytes", got, want)
	}
}
