package transport

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// fakeStore is an in-memory Storage whose per-op latency and failure injection
// are configurable, so the read-path robustness specs can drive the exact
// conditions observed in the wild (tail GET latency, throttled List, dead peer).
type fakeStore struct {
	mu    sync.Mutex
	files map[string][]byte

	downloadDelay time.Duration
	downloadErr   error

	listDelay time.Duration
	listErr   error

	gets  int
	lists int
}

func newFakeStore() *fakeStore {
	return &fakeStore{files: map[string][]byte{}}
}

func (f *fakeStore) Init(context.Context) error   { return nil }
func (f *fakeStore) EnsureDir(context.Context, string) error { return nil }
func (f *fakeStore) Upload(_ context.Context, path string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[path] = append([]byte(nil), data...)
	return nil
}
func (f *fakeStore) Download(_ context.Context, path string) ([]byte, error) {
	f.mu.Lock()
	f.gets++
	delay, err := f.downloadDelay, f.downloadErr
	data, ok := f.files[path]
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("NoSuchKey: %s", path)
	}
	return append([]byte(nil), data...), nil
}
func (f *fakeStore) List(_ context.Context, dir string, prefix string) ([]storage.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.listDelay > 0 {
		time.Sleep(f.listDelay)
	}
	var out []storage.FileInfo
	for name, data := range f.files {
		if !strings.HasPrefix(name, dir+"/") || !strings.HasPrefix(name, dir+"/"+prefix) {
			continue
		}
		out = append(out, storage.FileInfo{Name: name, Size: int64(len(data))})
	}
	return out, nil
}
func (f *fakeStore) Delete(_ context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.files, path)
	return nil
}
func (f *fakeStore) Watch(context.Context, string, time.Time, time.Duration) ([]storage.FileInfo, error) {
	return nil, nil
}

func (f *fakeStore) counters() (gets, lists int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets, f.lists
}

// --- M1-01: read GET timeout must stretch under tail latency -----------------

// Spec (beta observation 2026-10-09): under parallel download load the read
// path issued up to 48 concurrent S3 GETs. Tail latency pushed responses past
// the fixed 1200ms budget, so both hedged attempts died and the batch reported a
// hole that the watchdog then turned into a session teardown. The GET budget has
// to adapt to consecutive timeouts instead of being a fixed constant.
func TestReadGetTimeoutAdaptsToConsecutiveTimeouts(t *testing.T) {
	base := readTimeoutFloor()
	if base <= 0 {
		t.Fatalf("read timeout floor must be positive, got %v", base)
	}
	if base > 2*time.Second {
		t.Fatalf("floor should stay near the observed 1.2s budget, got %v", base)
	}

	// A healthy read must not inflate the budget.
	fresh := NewConn(ConnConfig{Store: newFakeStore(), SessionID: GenerateSessionID()})
	for i := 0; i < 3; i++ {
		if got := fresh.currentReadTimeout(); got != base {
			t.Fatalf("healthy read timeout = %v, want floor %v", got, base)
		}
	}

	// After consecutive timeouts the budget must grow, monotonically, up to a cap.
	load := NewConn(ConnConfig{Store: newFakeStore(), SessionID: GenerateSessionID()})
	prev := base
	for i := 0; i < 8; i++ {
		load.noteReadTimeout()
		got := load.currentReadTimeout()
		if got < prev {
			t.Fatalf("timeout budget shrank after timeout #%d: %v -> %v", i+1, prev, got)
		}
		if got > readTimeoutCeiling() {
			t.Fatalf("timeout budget %v exceeds ceiling %v", got, readTimeoutCeiling())
		}
		prev = got
	}
	if got := load.currentReadTimeout(); got <= base {
		t.Fatalf("under sustained tail latency the budget must grow above %v, got %v", base, got)
	}
	if got := load.currentReadTimeout(); got != readTimeoutCeiling() {
		t.Fatalf("sustained timeouts must saturate at the ceiling %v, got %v", readTimeoutCeiling(), got)
	}

	// A success must pull the budget back down so an idle session is not left
	// holding a multi-second timeout for the next interactive hop.
	load.noteReadOK()
	if got := load.currentReadTimeout(); got >= prev {
		t.Fatalf("a success must shrink the budget, got %v (prev %v)", got, prev)
	}
}

// The GET context must actually use the adaptive budget, not the old constant.
func TestDownloadUsesAdaptiveTimeout(t *testing.T) {
	c := NewConn(ConnConfig{Store: newFakeStore(), SessionID: GenerateSessionID()})
	for i := 0; i < 8; i++ {
		c.noteReadTimeout()
	}
	store := newFakeStore()
	store.downloadDelay = 250 * time.Millisecond
	store.files["sessions/abc/d_0"] = []byte("payload")
	c.Store = store

	// With the ceiling budget the 250ms response must arrive; with the old
	// 1.2s constant and heavy load this is the shape that failed. Assert the
	// hook exists and returns the adaptive value rather than a fixed one.
	if got := c.currentReadTimeout(); got != readTimeoutCeiling() {
		t.Fatalf("adaptive budget = %v, want ceiling %v", got, readTimeoutCeiling())
	}
	if _, err := c.downloadWithTimeout("sessions/abc/d_0"); err != nil {
		t.Fatalf("healthy download must succeed under the adaptive budget: %v", err)
	}
}

// --- M1-02: read concurrency must not stampede the backend -------------------

// Spec: 48 in-flight GETs is what drove the tail latency in the first place.
// The cap has to sit well below the read pool so the client stops being the
// cause of the stall it then blames on S3.
func TestReadConcurrencyIsBoundedBelowPoolSize(t *testing.T) {
	if maxReadConcurrency <= 0 {
		t.Fatalf("read concurrency must be positive, got %d", maxReadConcurrency)
	}
	if maxReadConcurrency > readPoolConnections {
		t.Fatalf("read concurrency %d must stay below the read pool (%d)", maxReadConcurrency, readPoolConnections)
	}
	// Comfortably above the single-stream need, well below the old stampede.
	if maxReadConcurrency > 24 {
		t.Fatalf("read concurrency %d is high enough to reproduce the GET burst", maxReadConcurrency)
	}
}

// A round must never have more GETs in flight than the cap, even when many
// files are present.
func TestFetchRoundNeverExceedsConcurrencyCap(t *testing.T) {
	store := newFakeStore()
	sessDir := "sessions/load"
	// 40 files present from readSeq on — far more than the cap.
	for seq := 0; seq < 40; seq++ {
		path := fmt.Sprintf("%s/%s", sessDir, SeqFileName("d_", uint64(seq)))
		if err := store.Upload(context.Background(), path, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	var peak, inFlight, mu sync.Mutex
	sync.Mutex{}
	store.downloadDelay = 5 * time.Millisecond

	c := NewConn(ConnConfig{Store: &countingStore{Storage: store, mu: &mu, peak: &peak, inFlight: &inFlight}, SessionDir: sessDir, SessionID: GenerateSessionID()})
	if n := c.fetchNext(); n == 0 {
		t.Fatalf("fetchNext consumed nothing; test setup is wrong")
	}

	mu.Lock()
	got := peak
	mu.Unlock()
	if got > maxReadConcurrency {
		t.Fatalf("peak concurrent GETs = %d, cap is %d", got, maxReadConcurrency)
	}
	if got < 2 {
		t.Fatalf("expected the round to run GETs concurrently, peak was %d", got)
	}
}

// --- M1-03: the hole watchdog must tolerate a slow producer ------------------

// Spec: holeTimeout was 7s, which fires while a loaded S3 is still PUTting a
// large file. The watchdog exists to catch a real flow-control deadlock, not to
// pre-empt a slow writer, so it has to sit above the slow-PUT budget.
func TestHoleTimeoutOutlastsSlowProducer(t *testing.T) {
	if holeTimeout <= uploadTimeout {
		t.Fatalf("holeTimeout %v must exceed uploadTimeout %v or it pre-empts a slow PUT", holeTimeout, uploadTimeout)
	}
	if holeTimeout < 20*time.Second {
		t.Fatalf("holeTimeout %v is too tight for a loaded backend; want >= 20s", holeTimeout)
	}
}

// A hole that fills before the watchdog must not tear the session down, and the
// session must survive an out-of-order arrival that takes longer than the old
// 7s budget.
func TestSlowLateArrivalDoesNotTearDownSession(t *testing.T) {
	store := newFakeStore()
	sessDir := "sessions/slow"
	c := NewConn(ConnConfig{Store: store, SessionDir: sessDir, SessionID: GenerateSessionID()})

	// seq 1 lands first (out-of-order), seq 0 is still "in flight" on a loaded
	// backend: a hole at readSeq=0 with later files present.
	payload1, err := c.encodeTestFile(1, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Upload(context.Background(), fmt.Sprintf("%s/%s", sessDir, SeqFileName("d_", 1)), payload1); err != nil {
		t.Fatal(err)
	}

	if n := c.fetchNext(); n != 0 {
		t.Fatalf("nothing contiguous to consume yet, got %d", n)
	}
	if c.holeSince.IsZero() {
		t.Fatalf("hole watchdog must arm on a hole with later files present")
	}
	// The producer lands late, but well before the new watchdog budget.
	go func() {
		time.Sleep(holeTimeout / 4)
		payload0, err := c.encodeTestFile(0, []byte("first"))
		if err != nil {
			return
		}
		_ = store.Upload(context.Background(), fmt.Sprintf("%s/%s", sessDir, SeqFileName("d_", 0)), payload0)
	}()

	deadline := time.Now().Add(holeTimeout / 2)
	for time.Now().Before(deadline) {
		if n := c.fetchNext(); n >= 2 {
			if !c.holeSince.IsZero() {
				t.Fatalf("a filled hole must disarm the watchdog")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("late arrival never became consumable — the read path is wedged")
}

// --- M1-04: ACK wait must not burn a flat 60s -------------------------------

// Spec: after a load-induced teardown the client polled the ACK file for a flat
// 60s, so a recovery took ~4 min (three such dials). The first ACK wait must be
// short and the retries must back off, so a wedged peer fails over fast.
func TestAckRetryScheduleIsBoundedAndBacksOff(t *testing.T) {
	sched := ackRetrySchedule()
	if len(sched) < 2 {
		t.Fatalf("ACK retry schedule must allow more than one attempt, got %d", len(sched))
	}
	if sched[0] > 10*time.Second {
		t.Fatalf("first ACK attempt deadline %v is too long; a wedged peer must fail over fast", sched[0])
	}
	total := time.Duration(0)
	for i, d := range sched {
		total += d
		if i > 0 && d < sched[i-1] {
			t.Fatalf("ACK schedule must not decrease: %v then %v", sched[i-1], d)
		}
	}
	if total > 90*time.Second {
		t.Fatalf("total ACK budget %v is worse than the old flat 60s", total)
	}
	// The last wait must be the longest, so a genuinely slow peer still gets a
	// real chance instead of being cut off after one second.
	if sched[len(sched)-1] <= sched[0] {
		t.Fatalf("final ACK wait %v must exceed the first %v", sched[len(sched)-1], sched[0])
	}
}

// --- M1-05: GET failures must be visible ------------------------------------

// Spec: during the incident the log showed no 429/timeout lines at all — the
// client swallowed every GET error, which is why the root cause took a fork to
// confirm. Transient GET failures have to be reported at a level an operator
// sees.
func TestReadGetFailuresAreLogged(t *testing.T) {
	store := newFakeStore()
	store.files["sessions/log/d_0"] = []byte("payload")
	store.downloadErr = fmt.Errorf("SlowDown: reduce your request rate")

	var out strings.Builder
	restore := captureLog(&out)
	c := NewConn(ConnConfig{Store: store, SessionDir: "sessions/log", SessionID: GenerateSessionID()})
	_, _ = c.downloadWithTimeout("sessions/log/d_0")
	restore()

	logged := out.String()
	if logged == "" {
		t.Fatalf("a failed GET must be logged; operators were blind to SlowDown errors")
	}
	if !strings.Contains(logged, "SlowDown") {
		t.Fatalf("log must carry the underlying error, got: %s", logged)
	}
	if !strings.Contains(logged, "read") {
		t.Fatalf("log must identify the failing direction, got: %s", logged)
	}
}

// countingStore records peak concurrency across the wrapped store's calls.
type countingStore struct {
	storage.Storage
	mu       *sync.Mutex
	peak     *int
	inFlight *int
}

func (s *countingStore) Download(ctx context.Context, path string) ([]byte, error) {
	s.mu.Lock()
	*s.inFlight++
	if *s.inFlight > *s.peak {
		*s.peak = *s.inFlight
	}
	s.mu.Unlock()
	time.Sleep(5 * time.Millisecond)
	data, err := s.Storage.Download(ctx, path)
	s.mu.Lock()
	*s.inFlight--
	s.mu.Unlock()
	return data, err
}