package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Relay is a TCP relay that can be told to stop moving bytes.
//
// The failure this exists to reproduce is specific: a backend that ACCEPTS the
// connection and then goes quiet. Closing the socket is not the same thing — the
// client sees a fast, obvious error and retries immediately. Staying silent is
// what pins a poll loop forever, and it is what the earlier gpucloud incident
// actually looked like: "timeout awaiting response headers", not "connection
// refused".
//
// So the freeze does not close anything. It stops the copies at the point where
// they would write, so the kernel receive buffers fill, the peer's TCP window
// closes, and the peer stalls exactly as it would against a server that had
// gone to sleep. Nothing on the wire says anything is wrong, which is the
// whole point.
type Relay struct {
	upstream string
	frozen   atomic.Bool

	mu    sync.Mutex
	conns int
}

func NewRelay(upstream string) *Relay { return &Relay{upstream: upstream} }

// Frozen reports whether relaying is currently suspended.
func (r *Relay) Frozen() bool { return r.frozen.Load() }

// SetFrozen suspends or resumes relaying.
func (r *Relay) SetFrozen(v bool) { r.frozen.Store(v) }

// Watch polls for the appearance and disappearance of a control file and flips
// the freeze accordingly.
//
// Polling rather than fsnotify on purpose: this is a test instrument, it runs
// for minutes, and a 50ms tick is far below the resolution of anything it is
// being used to measure. A missed notification here would silently invalidate
// a run; a 50ms late one cannot.
func (r *Relay) Watch(controlPath string, stop <-chan struct{}) {
	was := r.Frozen()
	r.SetFrozen(fileExists(controlPath))
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			now := fileExists(controlPath)
			if now != was {
				was = now
				r.SetFrozen(now)
				if now {
					logEvent("FREEZE — relaying suspended, sockets left open")
				} else {
					logEvent("THAW — relaying resumed")
				}
			}
		}
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// copy pumps src→dst, checking the freeze gate immediately BEFORE writing.
//
// The check has to sit before the write, not before the read: a read that is
// already parked in the kernel returns the moment data arrives, and a gate
// checked only on entry would let that first burst straight through. Holding
// the data instead is what fills the receive buffer and stalls the peer.
func (r *Relay) copy(dst io.Writer, src io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if n == 0 {
			continue
		}
		if !r.waitThawed() {
			return
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}

// waitThawed blocks while frozen and reports whether relaying may continue.
// A false return means the relay is shutting down.
func (r *Relay) waitThawed() bool {
	for r.Frozen() {
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

func (r *Relay) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go r.handle(conn)
	}
}

func (r *Relay) handle(conn net.Conn) {
	defer conn.Close()

	up, err := net.DialTimeout("tcp", r.upstream, 10*time.Second)
	if err != nil {
		logEvent("upstream dial failed: %v", err)
		return
	}
	defer up.Close()

	r.mu.Lock()
	r.conns++
	n := r.conns
	r.mu.Unlock()
	logEvent("conn #%d open (%s -> %s)", n, conn.RemoteAddr(), r.upstream)

	done := make(chan struct{}, 2)
	go func() { r.copy(up, conn); done <- struct{}{} }()
	go func() { r.copy(conn, up); done <- struct{}{} }()
	<-done

	r.mu.Lock()
	r.conns--
	r.mu.Unlock()
	logEvent("conn #%d closed (%d still open)", n, r.conns)
}

var logMu sync.Mutex

// logEvent writes a timestamped line in the same shape the tunnel logs use, so
// the soak's correlation reads the relay's FREEZE and THAW as first-class
// events sitting next to the stalls they caused.
func logEvent(format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	w := bufio.NewWriter(os.Stdout)
	fmt.Fprintf(w, "%s [faultrelay] %s\n",
		time.Now().Format("2006/01/02 15:04:05.000000"), fmt.Sprintf(format, args...))
	w.Flush()
}
