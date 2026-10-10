package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- the freeze has to actually stop bytes, not just claim to ---------------

// Spec: while frozen, data read from one side must NOT reach the other. This
// is the entire point of the instrument — a freeze that still forwarded would
// look correct in the log and test nothing.
func TestFrozenRelayStopsForwardingAndResumes(t *testing.T) {
	src, relayIn := net.Pipe()   // the "peer" writes here
	relayOut, sink := net.Pipe() // what the peer would receive
	defer src.Close()
	defer sink.Close()

	r := NewRelay("upstream:1")
	done := make(chan struct{})
	go func() { r.copy(relayOut, relayIn); close(done) }()

	// Unfrozen: bytes flow.
	go func() { src.Write([]byte("hello")) }()
	buf := make([]byte, 5)
	sink.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := sink.Read(buf); err != nil {
		t.Fatalf("unfrozen relay must forward: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("forwarded %q, want \"hello\"", buf)
	}

	// Frozen: the peer writes again and nothing must come out.
	r.SetFrozen(true)
	wrote := make(chan struct{})
	go func() { src.Write([]byte("frozen-payload")); close(wrote) }()
	<-wrote

	sink.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, err := sink.Read(buf); err == nil {
		t.Fatal("data reached the peer while frozen; the relay is not actually freezing")
	}

	// Thawed: the held data must eventually flow, proving nothing was lost and
	// the copy loop is parked rather than dead.
	r.SetFrozen(false)
	sink.SetReadDeadline(time.Now().Add(3 * time.Second))
	got := make([]byte, len("frozen-payload"))
	n, err := ioReadFull(sink, got)
	if err != nil {
		t.Fatalf("after thaw the relay must resume: %v", err)
	}
	if string(got[:n]) != "frozen-payload" {
		t.Fatalf("resumed with %q", got[:n])
	}

	// Teardown by closing the pipe, not by freezing. Freezing PARKS the copy
	// loop — that is the whole behaviour under test — so it will never return
	// on its own and using it here would hang the test for the wrong reason.
	src.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("copy did not return after its source closed")
	}
}

// --- the control file is the switch -----------------------------------------

func TestWatchFollowsTheControlFile(t *testing.T) {
	dir := t.TempDir()
	control := filepath.Join(dir, "freeze")

	r := NewRelay("upstream:1")
	stop := make(chan struct{})
	defer close(stop)
	go r.Watch(control, stop)

	// Absent: relaying normally.
	time.Sleep(120 * time.Millisecond)
	if r.Frozen() {
		t.Fatal("relay must start unfrozen when the control file is absent")
	}

	// Present: frozen. Touch is the whole user-facing interface of this tool,
	// so it has to work.
	if err := os.WriteFile(control, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitFor(func() bool { return r.Frozen() }, 2*time.Second)

	// Removed: thawed.
	if err := os.Remove(control); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return !r.Frozen() }, 2*time.Second) {
		t.Fatal("removing the control file must resume relaying")
	}
}

func waitFor(cond func() bool, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// ioReadFull is a local helper so the test does not pull io in twice.
func ioReadFull(c net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := c.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
