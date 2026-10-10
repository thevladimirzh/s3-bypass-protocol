package s3

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

var errTransient = errors.New("transient")

// The S3 client was built without an explicit Retryer, so the SDK installed
// retry.NewStandard(): three attempts, and a token bucket of 500 shared by
// every session on that client.
//
// Two consequences, and the second is the one that bites.
//
// The bucket refills only on successful attempts, so it empties under exactly
// the load it exists to smooth out. Once empty, GetRetryToken returns "failed
// to get rate limit token" instead of SlowDown, 503 or DeadlineExceeded — so
// the transport surfaces local bookkeeping in place of whatever the object
// store actually said. The read path decides whether to stretch its budget
// with errors.Is(err, context.DeadlineExceeded), which is false for a
// rate-limit error, so the budget stays pinned to its floor at the moment the
// backend is most overloaded, and each failing read looks like a hole to the
// watchdog.
//
// The three attempts also multiply under ours rather than replacing them:
// uploads are already hedged and retried until delivered, and the read poll
// loop re-reads on the next tick. Their first backoff can land outside the
// read budget anyway — billed and discarded.
func TestRetryerDoesNotRunOut(t *testing.T) {
	r := newStoreRetryer()

	// Well past the SDK default bucket of 500.
	const attempts = 5000
	for i := 0; i < attempts; i++ {
		release, err := r.GetRetryToken(context.Background(), errTransient)
		if err != nil {
			t.Fatalf("retry token refused at attempt %d: %v", i, err)
		}
		if release != nil {
			_ = release(nil)
		}
	}
}

// Our layers already own the retry policy — hedged PUTs, retry-until-delivered
// uploads, and a poll loop that re-reads on the next tick. What the SDK adds on
// top multiplies the wire requests per logical operation and cannot fit inside
// the budget those layers enforce.
func TestSDKDoesNotRetryUnderneathUs(t *testing.T) {
	if got := newStoreRetryer().MaxAttempts(); got != 1 {
		t.Errorf("MaxAttempts = %d, want 1 — the layers above own the retry decision", got)
	}
}

var _ aws.Retryer = newStoreRetryer()
