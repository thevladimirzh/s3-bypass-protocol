package s3

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// The S3 client was built without an explicit Retryer, so the SDK installed
// retry.NewStandard() for us: three attempts, and a 500-token bucket shared by
// every session on that client.
//
// When the bucket empties, GetRetryToken returns "failed to get rate limit
// token" instead of SlowDown, 503 or DeadlineExceeded. The read path keys its
// adaptive budget off DeadlineExceeded, so from that point it stops stretching
// — precisely when the backend is overloaded and stretching is what the budget
// is for. Every read then reads as a hole and the watchdog tears the session
// down.
//
// The bucket refills only on successful attempts, so it drains exactly when
// things are already going badly.
func TestRetryerDoesNotRunOut(t *testing.T) {
	r := newStoreRetryer()

	// Well past the SDK default bucket of 500.
	const attempts = 5000
	for i := 0; i < attempts; i++ {
		if _, err := r.GetAttemptToken(context.Background()); err != nil {
			t.Fatalf("attempt token refused at attempt %d: %v", i, err)
		}
		if _, err := r.GetRetryToken(context.Background()); err != nil {
			t.Fatalf("retry token refused at attempt %d: %v", i, err)
		}
		r.ReleaseRetryToken(context.Background())
	}
}

// Our layers already own the retry policy — hedged PUTs, retry-until-delivered
// uploads, and a poll loop that re-reads on the next tick. The SDK stacking
// three more attempts underneath multiplies the wire requests per logical
// operation, and its first retry can land outside the read budget entirely.
func TestSDKDoesNotRetryUnderneathUs(t *testing.T) {
	r := newStoreRetryer()

	attempts := 0
	_, err := r.Retry(context.Background(),
		func(ctx context.Context, _ int) error {
			attempts++
			return retryableError{}
		},
		func(ctx context.Context) (time.Duration, error) {
			return 10 * time.Millisecond, nil
		})

	if err == nil {
		t.Fatal("expected the operation to fail")
	}
	if attempts != 1 {
		t.Errorf("a retryable error produced %d attempts, want 1 — our own loop decides whether to try again", attempts)
	}
}

type retryableError struct{}

func (retryableError) Error() string   { return "transient" }
func (retryableError) RetryableError() {}

var _ aws.Retryer = newStoreRetryer()
