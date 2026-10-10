package transport

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
	"github.com/xtls/xray-core/proxy/fedarisha/storage/local"
)

// countingStorage counts the hello probes that are issued per path.
type countingStorage struct {
	storage.Storage
	probes atomic.Int64
}

func (s *countingStorage) Download(ctx context.Context, path string) ([]byte, error) {
	s.probes.Add(1)
	return s.Storage.Download(ctx, path)
}

// A session directory that has no hello is a normal state: the client creates
// the directory first and writes the hello a moment later. The problem is what
// happens when it never arrives.
//
// acceptSession returned early without recording the directory, so every poll
// tick re-read the hello — a GET that can only ever answer 404. With a server
// configured at pollIntervalMs=100 and a few dozen abandoned directories left
// by clients that died without closing, that is hundreds of guaranteed-miss
// requests per second, sustained indefinitely. The new session's hello then
// queues behind them, which is what a 15s handshake is: not a slow peer, a
// saturated pool. The cost is billed per request and the latency lands on
// exactly the handshake it was supposed to serve.
func TestStaleSessionDirectoryStopsBeingProbed(t *testing.T) {
	root := t.TempDir()
	sessDir := filepath.Join(root, DefaultSessionsDir, "abandoned")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatal(err)
	}

	store := &countingStorage{Storage: local.New(local.Config{RootDir: root})}
	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// Shrink the window rather than sleeping through the real one: the value
	// is a package var precisely so this is possible, and a test that spends
	// a minute proving a timer works protects nothing.
	shrinkProbeWindow(t, 40*time.Millisecond)

	// Age the directory past the window a client would realistically take to
	// write its hello, so the second probe is the one that should retire it.
	listener.probedBefore(t, "sessions/abandoned")
	probesAfterFirst := store.probes.Load()

	time.Sleep(60 * time.Millisecond)

	for i := 0; i < 20; i++ {
		listener.acceptSession("sessions/abandoned")
	}
	if got := store.probes.Load() - probesAfterFirst; got > 1 {
		t.Fatalf("stale directory probed %d more times after the window elapsed, want at most 1 (the probe that retires it)", got)
	}

	// And once retired it stays retired — this is the assertion that fails
	// against the old behaviour, where every tick probed again forever.
	before := store.probes.Load()
	for i := 0; i < 50; i++ {
		listener.scanSessionsIn(DefaultSessionsDir)
	}
	if got := store.probes.Load() - before; got != 0 {
		t.Errorf("retired directory was probed %d more times, want 0", got)
	}
}

// A directory that is merely young must still be probed. Marking it known on
// the first miss would break the normal handshake, where the server sees the
// directory before the hello lands.
func TestYoungSessionDirectoryIsStillProbed(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, DefaultSessionsDir, "fresh"), 0o755); err != nil {
		t.Fatal(err)
	}

	store := &countingStorage{Storage: local.New(local.Config{RootDir: root})}
	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	before := store.probes.Load()
	for i := 0; i < 5; i++ {
		listener.acceptSession("sessions/fresh")
	}
	if got := store.probes.Load() - before; got != 5 {
		t.Errorf("young directory probed %d times, want 5 — a hello that is still in flight must not be skipped", got)
	}

	listener.knownMu.Lock()
	known := listener.known["sessions/fresh"]
	listener.knownMu.Unlock()
	if known {
		t.Error("a young directory was marked known before its window elapsed")
	}
}

// probedBefore runs a scan until the directory has been probed at least once,
// so the test does not depend on the listener's ticker having fired.
func (l *Listener) probedBefore(t *testing.T, sessDir string) {
	t.Helper()
	for i := 0; i < 20; i++ {
		l.acceptSession(sessDir)
		if l.storeProbes(t) > 0 {
			return
		}
	}
	t.Fatal("directory was never probed")
}

func (l *Listener) storeProbes(t *testing.T) int64 {
	t.Helper()
	store, ok := l.Store.(*countingStorage)
	if !ok {
		t.Fatal("listener is not backed by countingStorage")
	}
	return store.probes.Load()
}

func shrinkProbeWindow(t *testing.T, d time.Duration) {
	t.Helper()
	original := staleSessionProbeWindow
	staleSessionProbeWindow = d
	t.Cleanup(func() { staleSessionProbeWindow = original })
}
