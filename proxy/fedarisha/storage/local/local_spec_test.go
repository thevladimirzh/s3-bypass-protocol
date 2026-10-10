package local

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// S3 commits an object atomically: it does not appear in a listing until the
// body is complete. os.WriteFile truncates first and writes second, so a
// concurrent reader can list the file at zero length — or part way — and read a
// truncated body.
//
// The protocol's reader is List-then-GET and never validates size, so this is
// not a theoretical divergence. And uploadWithTimeout fires up to three
// concurrent Upload calls for the same path, which against this backend is
// three concurrent os.WriteFile on one file.
//
// Test 1 is the primitive: a reader must never observe a partial object.
func TestUploadedObjectIsNeverPartiallyVisible(t *testing.T) {
	root := t.TempDir()
	store := New(Config{RootDir: root})
	const path = "sessions/atomic/file"
	if err := store.EnsureDir(context.Background(), "sessions/atomic"); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 512*1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	if err := store.Upload(context.Background(), path, payload); err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(root, "sessions", "atomic", "file")
	before, err := os.Stat(objectPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Upload(context.Background(), path, payload); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(objectPath)
	if err != nil {
		t.Fatal(err)
	}

	// Overwriting must replace the object, not truncate it in place. The
	// distinction is the file's identity: os.WriteFile reuses the existing
	// inode and truncates it, so a concurrent List-then-GET reader can observe
	// the object at some intermediate size. Writing to a temporary file and
	// renaming gives it a new identity, and a reader sees either the whole old
	// object or the whole new one.
	//
	// Asserted through os.SameFile rather than by sampling sizes, because
	// sampling missed the window even at 32MB — the truncation is real but far
	// shorter than a poll loop can reliably catch.
	if runtime.GOOS == "windows" {
		// Go's Windows implementation of SameFile falls back to comparing path
		// strings when the file ID cannot be read, and then reports "same file"
		// for any two stats of the same path — including one taken before a
		// rename replaced it. The assertion cannot distinguish the two
		// implementations, so it is skipped rather than quietly inverted.
		t.Skip("os.SameFile cannot observe replacement on Windows")
	}
	if os.SameFile(before, after) {
		t.Error("the object was truncated in place rather than replaced; " +
			"a concurrent reader can observe a partially written object")
	}

	// Portable companion: whatever the platform, an upload must not leave its
	// scratch file behind.
	entries, err := os.ReadDir(filepath.Dir(objectPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), uploadTempPrefix) {
			t.Errorf("upload left %q behind", e.Name())
		}
	}

	got, err := store.Download(context.Background(), path)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("stored %d bytes, want %d", len(got), len(payload))
	}
}

// The traversal guard is a raw string-prefix compare, so a sibling directory
// whose name merely starts with the root passes it: root "/var/fed" accepts
// "/var/fed-backup/x".
//
// It also panics rather than returning an error, and every storage method goes
// through abs(), so one unexpected path takes down the process instead of
// surfacing where it can be logged.
func TestPathGuardAcceptsSiblingAndDoesNotPanic(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fed")
	sibling := root + "-backup"
	if err := os.MkdirAll(sibling, 0o755); err != nil {
		t.Fatal(err)
	}
	store := New(Config{RootDir: root})

	err := store.Upload(context.Background(), filepath.Join("..", filepath.Base(sibling), "escaped"), []byte("x"))
	if err == nil {
		t.Fatal("a write outside the root succeeded")
	}
}

func TestPathGuardPanicsInsteadOfReturningAnError(t *testing.T) {
	root := t.TempDir()
	store := New(Config{RootDir: root})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a traversal attempt panicked the process: %v", r)
		}
	}()

	// A path that genuinely leaves the root once cleaned.
	_ = store.Upload(context.Background(), ".."+string(filepath.Separator)+"escaped", []byte("x"))
}

func TestPathGuardStillAllowsNormalPaths(t *testing.T) {
	root := t.TempDir()
	store := New(Config{RootDir: root})
	ctx := context.Background()

	if err := store.EnsureDir(ctx, "sessions"); err != nil {
		t.Fatal(err)
	}
	if err := store.Upload(ctx, "sessions/file", []byte("payload")); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	got, err := store.Download(ctx, "sessions/file")
	if err != nil || string(got) != "payload" {
		t.Fatalf("Download = %q, %v", got, err)
	}
}

var _ = time.Second
