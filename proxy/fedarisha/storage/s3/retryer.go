package s3

import (
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
)

// newStoreRetryer returns the retry policy for object-store calls, which is
// deliberately no retry at all.
//
// Left unset, the SDK installs retry.NewStandard(): three attempts behind a
// 500-token bucket shared by every session on the client. Two things go wrong
// with that here, and neither is visible without knowing how it works.
//
// The bucket refills only on successful attempts, so it empties under exactly
// the load it exists to smooth out. Once empty, GetRetryToken returns
// "failed to get rate limit token" — and that is the error the transport sees.
// Not SlowDown, not 503, not DeadlineExceeded. The read path decides whether to
// stretch its budget with errors.Is(err, context.DeadlineExceeded), which is
// false for a rate-limit error, so the budget stays pinned to its floor at the
// moment the backend is most overloaded and each failing read looks like a hole
// to the watchdog.
//
// The three attempts also multiply under ours rather than replacing them:
// uploads are already hedged and retried until delivered, and the read poll
// loop re-reads on the next tick. Three SDK attempts per logical call triple
// the billed request count, and the first retry's backoff can land outside the
// read budget anyway — billed and discarded.
//
// The layers above this one own the decision because they know the budget. A
// caller with no retry policy is not left without one: each caller here either
// retries with its own pacing (uploadUntilDelivered, uploadRetrying) or simply
// tries again on its next poll tick.
//
// RateLimiter is set rather than nil because GetRetryToken dereferences it
// unconditionally, even with zero costs.
func newStoreRetryer() aws.Retryer {
	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = 1
		o.RetryCost = 0
		o.RetryTimeoutCost = 0
		o.RateLimiter = ratelimit.NewTokenRateLimit(math.MaxInt32)
	})
}
