package transport

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// --- M2: List had no deadline and no diagnostics --------------------------
//
// fetchNext passed the connection's own context to the store's List. That
// context lives for the whole session, so a List that never returns — a
// backend that accepted the TCP connection and then went silent, which is
// what a load balancer dropping a wedged RGW pod looks like — pinned the poll
// loop for good. The poll loop is the only thing that drives the read
// direction, so the tunnel stops, the connection still looks alive, and
// nothing is written to the log to say why.
//
// The error case was the mirror image: `return 0` with a comment calling it
// transient. A transient error is worth retrying, but a permanent one —
// denied ListBucket on the restricted credentials this project deploys with,
// a bucket that is gone — produced the same silence, retried every 90ms
// forever, at real cost per request, with zero evidence in the log.

// deadlineStore reports whether the context it was handed carries a deadline,
// and otherwise defers to the wrapped store. Real backends honour ctx (the
// S3 SDK does), so a fake that ignores it cannot stand in for one: a timeout
// spec written against a ctx-ignoring fake would fail even after the fix.
type deadlineStore struct {
	storage.Storage

	mu       sync.Mutex
	deadline time.Time
	had      bool
}

func (d *deadlineStore) List(ctx context.Context, dir, prefix string) ([]storage.FileInfo, error) {
	dl, ok := ctx.Deadline()
	d.mu.Lock()
	d.had = ok
	d.deadline = dl
	d.mu.Unlock()
	return d.Storage.List(ctx, dir, prefix)
}

func (d *deadlineStore) sawDeadline() (bool, time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.had, d.deadline
}

// Spec: the List call must carry a deadline of its own. The session context
// cannot supply one — it is meant to outlive any single call, so bounding List
// with it bounds nothing.
func TestListCarriesItsOwnDeadline(t *testing.T) {
	store := &deadlineStore{Storage: newFakeStore()}
	c := NewConn(ConnConfig{Store: store, SessionDir: "sessions/deadline", SessionID: GenerateSessionID()})
	defer c.Close()

	c.fetchNext()

	had, dl := store.sawDeadline()
	if !had {
		t.Fatalf("List was handed a context with no deadline; a silent backend pins the read path forever")
	}
	if remaining := time.Until(dl); remaining <= 0 {
		t.Fatalf("List deadline %v has already expired", remaining)
	} else if remaining > 60*time.Second {
		t.Fatalf("List deadline is %v away; too loose to unstick a wedged backend", remaining)
	}
}

// stallingListStore blocks inside List until either its context expires or the
// stall is released — the shape of a backend that accepted the connection and
// went quiet.
type stallingListStore struct {
	storage.Storage

	mu      sync.Mutex
	release chan struct{}
	entered int
}

func (s *stallingListStore) List(ctx context.Context, dir, prefix string) ([]storage.FileInfo, error) {
	s.mu.Lock()
	s.entered++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
		return s.Storage.List(ctx, dir, prefix)
	}
}

func (s *stallingListStore) stalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entered
}

func (s *stallingListStore) unstall() { close(s.release) }

// Spec: a backend that stalls every List must not take the session with it.
// The read path has to keep calling List while the backend is silent — that is
// what proves each attempt was cut loose by a deadline rather than pinned —
// and then deliver on its own once the backend answers again. The caller never
// re-dials, so a session that cannot recover by itself stays broken until
// someone notices.
func TestReadPathRecoversWhenTheBackendStallsOnList(t *testing.T) {
	restoreTimeout := shortenListTimeout(250 * time.Millisecond)
	defer restoreTimeout()

	inner := newFakeStore()
	store := &stallingListStore{Storage: inner, release: make(chan struct{})}
	sessDir := "sessions/stalled-list"
	c := NewConn(ConnConfig{Store: store, SessionDir: sessDir, SessionID: GenerateSessionID()})
	defer c.Close()

	drained := make(chan string, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := c.Read(buf)
		if err != nil {
			drained <- ""
			return
		}
		drained <- string(buf[:n])
	}()

	// Nothing is planted yet, so every List stalls. The count climbing is the
	// observation that matters: pinned on the first call it stays at one
	// forever, and the session is silently dead while still looking alive.
	const wantAttempts = 3
	deadline := time.Now().Add(15 * time.Second)
	for store.stalls() < wantAttempts && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := store.stalls(); got < wantAttempts {
		t.Fatalf("List was attempted %d time(s) during a stall; each call must be cut loose by a deadline "+
			"so a silent backend cannot pin the read path", got)
	}

	data, err := c.encodeTestFile(0, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	path := sessDir + "/" + SeqFileName(c.readPrefix, 0)
	if err := inner.Upload(context.Background(), path, data); err != nil {
		t.Fatal(err)
	}
	store.unstall()

	select {
	case got := <-drained:
		if got != "payload" {
			t.Fatalf("recovered read returned %q, want %q", got, "payload")
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("read path never recovered after the backend stopped stalling")
	}
}

// Spec: a List that fails is evidence about the backend and must reach the
// operator. It was dropped on the floor, which is why a session that stopped
// moving produced no line anywhere explaining it.
func TestListFailuresAreLogged(t *testing.T) {
	store := newFakeStore()
	store.setListErr(errors.New("AccessDenied: ListBucket not allowed"))

	var out strings.Builder
	restore := captureLog(&out)
	c := NewConn(ConnConfig{Store: store, SessionDir: "sessions/listlog", SessionID: GenerateSessionID()})
	c.fetchNext()
	restore()
	c.Close()

	logged := out.String()
	if logged == "" {
		t.Fatalf("a failed List must be logged; a permanent error was retried silently forever")
	}
	if !strings.Contains(logged, "AccessDenied") {
		t.Fatalf("log must carry the underlying error, got: %s", logged)
	}
	if !strings.Contains(logged, "list") && !strings.Contains(logged, "List") {
		t.Fatalf("log must identify List as the failing call, got: %s", logged)
	}
}
