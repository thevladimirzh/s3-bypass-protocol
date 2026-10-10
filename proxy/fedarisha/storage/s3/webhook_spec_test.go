package s3

import (
	"net/http"
	"testing"
	"time"
)

// SetupWebhook was the only call in the tree with no timeout on any layer: it
// used http.DefaultClient, whose Timeout is 0, on a transport with no
// ResponseHeaderTimeout. It runs synchronously during inbound init, before the
// listener starts, so a store that accepts the connection and then says
// nothing — a wedged notification call, a proxy in front of one — means the
// inbound never comes up, with nothing in the log to say why.
//
// The default client is process-global, so it is the wrong thing to borrow
// twice over: anything else in the program can change our timeout, or inherit
// ours.
//
// This asserts the property rather than the hang. A network-level hang test is
// not constructible here: the URL is built as {bucket}.{host}, which does not
// resolve against an httptest server, so the request fails in milliseconds and
// proves nothing about how long a real one would wait.
func TestWebhookClientBoundsItsOwnWait(t *testing.T) {
	client := webhookHTTPClient()

	if client == http.DefaultClient {
		t.Fatal("the webhook client is the process-global default client")
	}
	if client.Timeout <= 0 {
		t.Error("the webhook client has no overall timeout")
	} else if client.Timeout > time.Minute {
		t.Errorf("webhook timeout is %v, too long to keep inbound init from hanging", client.Timeout)
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport type %T", client.Transport)
	}
	if transport.ResponseHeaderTimeout <= 0 {
		t.Error("the webhook transport has no ResponseHeaderTimeout: a server that accepts and never replies waits forever")
	}
}

// A caller that supplies a context must still be bounded by the client: the
// init path passes a context that outlives any sensible wait.
func TestWebhookClientTimeoutIsIndependentOfCallerContext(t *testing.T) {
	if got := webhookHTTPClient().Timeout; got <= 0 {
		t.Fatalf("client timeout is %v", got)
	}
}
