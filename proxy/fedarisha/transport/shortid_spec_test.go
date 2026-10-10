package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
	"github.com/xtls/xray-core/proxy/fedarisha/storage/local"
)

// The session id is the last path segment of a directory under the sessions
// prefix, so its length is set by whoever wrote the object, not by the server.
// Seven log lines in acceptSession sliced it to eight characters unguarded, and
// a directory named "x" with a well-formed hello panicked the process — which,
// unrecovered in the listener's goroutine, takes every inbound and every user
// with it.
//
// This is not a robustness nicety: writing one directory is enough.
func TestShortSessionIDDoesNotPanic(t *testing.T) {
	for _, name := range []string{"x", "ab", "short12", "12345678x", "exactly8"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, DefaultSessionsDir, name)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			// Long enough to pass the key-extraction check: sessID + 32 bytes.
			hello := append([]byte(name), make([]byte, 32)...)
			if err := os.WriteFile(filepath.Join(dir, HelloFile), hello, 0o644); err != nil {
				t.Fatal(err)
			}

			listener, err := Listen(context.Background(),
				local.New(local.Config{RootDir: root}), DefaultSessionsDir,
				ListenOpts{PollInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("acceptSession panicked on session id %q: %v", name, r)
				}
			}()

			listener.acceptSession(DefaultSessionsDir + "/" + name)
		})
	}
}

// The same id also reaches the log from the entitlement gate, which runs
// before the hello is ever fetched.
func TestShortSessionIDAtEntitlementGateDoesNotPanic(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, DefaultSessionsDir, "x"), 0o755); err != nil {
		t.Fatal(err)
	}

	listener, err := ListenMultiUser(context.Background(),
		local.New(local.Config{RootDir: root}), "",
		ListenOpts{
			PollInterval:  time.Hour,
			IsUserAllowed: func(string) bool { return false },
		})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("entitlement gate panicked on a short session id: %v", r)
		}
	}()

	listener.acceptSession("x/sessions/x")
}

// A short id must still be logged in full — truncated for a log line is fine,
// dropped is not.
func TestShortSessionIDIsStillIdentifiable(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, DefaultSessionsDir, "abc")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	hello := append([]byte("abc"), make([]byte, 32)...)
	if err := os.WriteFile(filepath.Join(dir, HelloFile), hello, 0o644); err != nil {
		t.Fatal(err)
	}

	store := local.New(local.Config{RootDir: root})
	listener, err := Listen(context.Background(), store, DefaultSessionsDir,
		ListenOpts{PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	listener.acceptSession(DefaultSessionsDir + "/abc")

	// The session was accepted, so it must be discoverable under its real name.
	listener.knownMu.Lock()
	known := listener.known[DefaultSessionsDir+"/abc"]
	listener.knownMu.Unlock()
	if !known {
		t.Error("session was not recorded under its own id")
	}

	var _ storage.Storage = store
}

// The id does not stop at acceptSession: the listener hands the same
// externally-named id to NewConn, whose own logging slices it again. Fixing
// only the listener moves the panic one frame down — which is what happens the
// moment the incoming channel is full and the dropped session is closed.
func TestShortSessionIDSurvivesConnLifecycle(t *testing.T) {
	conn := NewConn(ConnConfig{
		Store:      local.New(local.Config{RootDir: t.TempDir()}),
		SessionID:  "ab",
		SessionDir: "sessions/ab",
		IsClient:   false,
	})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Conn.Close panicked on a short session id: %v", r)
		}
	}()

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
