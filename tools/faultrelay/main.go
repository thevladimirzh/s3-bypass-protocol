package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var packageDoc = `
faultrelay sits between a fedarisha client and its S3 endpoint and can be told
to stop moving bytes on demand, without closing anything.

Why: the recovery path can only be verified against the failure it exists for.
Waiting for a backend to misbehave is not a plan — the gpucloud timeout that
motivated half the diagnostics in this fork happened once in an afternoon.
This makes it happen on command.

The freeze is deliberately NOT a disconnect. A client that sees "connection
refused" retries immediately and never learns anything about how it behaves
against a peer that accepted the connection and went quiet — which is the case
that used to pin the read path forever. Here the sockets stay open and the bytes
simply stop moving, so the receive buffers fill and the peer stalls exactly as it
would against a server that had fallen asleep.

	go run ./tools/faultrelay -upstream s3.example.ru:443 -control /tmp/freeze
	touch /tmp/freeze     # backend goes quiet
	rm /tmp/freeze       # backend answers again

DNS must already point the endpoint's hostname at the relay's listen address
before the client starts, so that the TLS certificate still validates against
the real hostname. Pass -upstream the resolved address, not the hostname, or
the relay would resolve back to itself.
`

var (
	listenAddr = flag.String("listen", "127.0.0.1:15432", "address to accept relay connections on")
	upstream   = flag.String("upstream", "", "resolved upstream address, host:port (REQUIRED)")
	control    = flag.String("control", "/tmp/fedarisha-freeze", "path whose presence freezes relaying")
)

func main() {
	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), packageDoc)
		fmt.Fprintln(flag.CommandLine.Output(), "flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *upstream == "" {
		fmt.Fprintln(os.Stderr, "error: -upstream is required and must be a resolved host:port")
		os.Exit(2)
	}
	if _, _, err := net.SplitHostPort(*upstream); err != nil {
		fmt.Fprintf(os.Stderr, "error: -upstream %q is not host:port\n", *upstream)
		os.Exit(2)
	}

	// A stale control file would freeze the relay before the first connection
	// and look exactly like an unexplained total outage.
	if fileExists(*control) {
		fmt.Fprintf(os.Stderr, "error: %s already exists; remove it or the relay starts frozen\n", *control)
		os.Exit(2)
	}

	l, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot listen on %s: %v\n", *listenAddr, err)
		os.Exit(1)
	}

	r := NewRelay(*upstream)

	stop := make(chan struct{})
	go r.Watch(*control, stop)

	logEvent("listening on %s -> %s (control file: %s)", *listenAddr, *upstream, *control)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		logEvent("shutting down")
		close(stop)
		l.Close()
	}()

	if err := r.Serve(l); err != nil {
		// Close after a signal is the normal path out, not a failure.
		os.Exit(0)
	}
}
