package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage/local"
)

// Both maps are keyed by session directory and both only ever grew. On a
// server that runs for months that is not a slow leak, it is a slow OOM: the
// process is killed with no clear cause, restarts, and looks like an
// unexplained crash-loop.
//
// The premise of each entry is that the directory still exists. When it does
// not — cleanupSession finished, or the client died and the lifecycle rule
// swept it — the entry is not merely useless, it is what keeps the entry alive.

func TestKnownEntriesArePrunedWhenTheDirectoryIsGone(t *testing.T) {
	root := t.TempDir()
	store := local.New(local.Config{RootDir: root})
	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	listener.knownMu.Lock()
	listener.known["sessions/aaa"] = true
	listener.known["sessions/bbb"] = true
	listener.knownMu.Unlock()

	// The sessions dir does not exist at all — both entries are for
	// directories that are gone.
	listener.scanSessionsIn(DefaultSessionsDir)

	listener.knownMu.Lock()
	remaining := len(listener.known)
	listener.knownMu.Unlock()
	if remaining != 0 {
		t.Errorf("known kept %d entries for directories that no longer exist", remaining)
	}
}

func TestKnownEntriesForLiveDirectoriesSurvive(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, DefaultSessionsDir, "live"), 0o755); err != nil {
		t.Fatal(err)
	}
	store := local.New(local.Config{RootDir: root})
	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	listener.knownMu.Lock()
	listener.known["sessions/live"] = true
	listener.knownMu.Unlock()

	listener.scanSessionsIn(DefaultSessionsDir)

	listener.knownMu.Lock()
	_, still := listener.known["sessions/live"]
	listener.knownMu.Unlock()
	if !still {
		t.Error("a live session directory was pruned from known")
	}
}

// Multi-user pruning must not touch a sibling user: each user's sessions are
// scanned independently, and one user's scan must not conclude that another
// user's directory is gone.
func TestPruningIsScopedToTheScannedUser(t *testing.T) {
	root := t.TempDir()
	for _, u := range []string{"alice", "bob"} {
		if err := os.MkdirAll(filepath.Join(root, u, DefaultSessionsDir, "sess"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	store := local.New(local.Config{RootDir: root})
	listener, err := ListenMultiUser(context.Background(), store, "",
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	for _, u := range []string{"alice", "bob"} {
		listener.knownMu.Lock()
		listener.known[u+"/sessions/sess"] = true
		listener.knownMu.Unlock()
	}

	// Scan one user only.
	listener.scanSessionsIn("alice/" + DefaultSessionsDir)

	listener.knownMu.Lock()
	_, aliceKept := listener.known["alice/sessions/sess"]
	_, bobKept := listener.known["bob/sessions/sess"]
	listener.knownMu.Unlock()

	if !aliceKept {
		t.Error("alice's live session was pruned by her own scan")
	}
	if !bobKept {
		t.Error("bob's session was pruned by a scan of another user")
	}
}

// The retirement path marks a directory known and returns. Leaving its
// noHelloSince entry behind was the regression introduced with the fix for the
// handshake delay: every abandoned directory leaks one entry, which is exactly
// the population that fix was written for.
func TestNoHelloSinceIsDroppedWhenADirectoryIsRetired(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, DefaultSessionsDir, "abandoned"), 0o755); err != nil {
		t.Fatal(err)
	}
	store := local.New(local.Config{RootDir: root})
	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	shrinkProbeWindow(t, 40*time.Millisecond)

	listener.acceptSession("sessions/abandoned") // first miss starts the clock
	time.Sleep(60 * time.Millisecond)
	listener.acceptSession("sessions/abandoned") // retires it

	listener.knownMu.Lock()
	known := listener.known["sessions/abandoned"]
	listener.knownMu.Unlock()
	if !known {
		t.Fatal("precondition: the directory should have been retired")
	}

	listener.noHelloMu.Lock()
	_, leaked := listener.noHelloSince["sessions/abandoned"]
	listener.noHelloMu.Unlock()
	if leaked {
		t.Error("a retired directory kept its noHelloSince entry")
	}
}
