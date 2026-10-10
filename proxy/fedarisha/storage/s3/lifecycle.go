package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	lifecycleRuleIDPrefix = "fedarisha-expire-"
	lifecycleExpireDays   = int32(1)
)

// SetupLifecycle ensures the bucket has a lifecycle rule that expires objects
// under prefix one day after their last modification. Idempotent: re-running
// rewrites only the rule keyed by this prefix and preserves rules belonging
// to other prefixes (so multiple inbounds can share a bucket).
//
// Days=1 is the AWS S3 minimum; active fedarisha sessions live seconds-to-
// minutes (server consumes and deletes), so they never reach the threshold —
// the rule only sweeps orphans (crashed clients, unconsumed multi-user dirs).
func (s *S3Store) SetupLifecycle(ctx context.Context, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("lifecycle: prefix is required")
	}

	ruleID := lifecycleRuleIDForPrefix(prefix)

	existing, err := s.fetchLifecycleRules(ctx)
	if err != nil {
		return err
	}

	rules := make([]s3types.LifecycleRule, 0, len(existing)+1)
	// VK Cloud rejects AbortIncompleteMultipartUpload with "InvalidArgument:
	// This argument is unsupported at the time" — keep the rule to just the
	// fields VK Cloud accepts (Filter.Prefix + Expiration.Days).
	rules = append(rules, s3types.LifecycleRule{
		ID:     aws.String(ruleID),
		Status: s3types.ExpirationStatusEnabled,
		Filter: &s3types.LifecycleRuleFilter{
			Prefix: aws.String(prefix),
		},
		Expiration: &s3types.LifecycleExpiration{
			Days: aws.Int32(lifecycleExpireDays),
		},
	})
	for _, r := range existing {
		if aws.ToString(r.ID) == ruleID {
			continue
		}
		rules = append(rules, r)
	}

	_, err = s.readClient.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.cfg.Bucket),
		LifecycleConfiguration: &s3types.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})
	if err != nil {
		return fmt.Errorf("put bucket lifecycle: %w", err)
	}
	return nil
}

func (s *S3Store) fetchLifecycleRules(ctx context.Context) ([]s3types.LifecycleRule, error) {
	out, err := s.readClient.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.cfg.Bucket),
	})
	if err == nil {
		return out.Rules, nil
	}
	// "no rules yet" is the success case for the first run on a fresh bucket.
	// Check via the smithy-go APIError interface so we don't pin the import.
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchLifecycleConfiguration" {
		return nil, nil
	}
	return nil, fmt.Errorf("get bucket lifecycle: %w", err)
}

// lifecycleRuleIDForPrefix derives a rule ID from the inbound's S3 prefix so
// that multiple inbounds sharing a bucket get separate, addressable rules.
// lifecycleRuleIDForPrefix derives a stable, unique rule ID from a prefix.
//
// Two constraints shape the encoding. VK Cloud accepts only alphanumerics plus
// -_. in a rule ID, so a hash alone is not available — and a rejected ID means
// no rule at all rather than a differently-named one. And the mapping has to be
// injective, because SetupLifecycle reconciles by ID: two prefixes that produce
// the same ID means the second inbound to start silently deletes the first's
// expiry rule.
//
// Collapsing separators to dashes was many-to-one. "team-a/prod/" and
// "team-a-prod/" both produced "fedarisha-expire-team-a-prod", so the second
// inbound removed the first's rule and left that prefix with orphans retained
// and billed forever. Separator and dash are both ordinary characters in a
// directory name, and the multi-user layout puts prefixes exactly one segment
// apart, so this was reachable with ordinary configuration.
//
// So anything outside the accepted set is hex-encoded with '_' as the marker,
// and the marker is escaped along with everything else. The result stays inside
// VK Cloud's character set, because a byte outside it becomes three bytes
// inside it.
func lifecycleRuleIDForPrefix(prefix string) string {
	cleaned := strings.Trim(prefix, "/")
	if cleaned == "" {
		return lifecycleRuleIDPrefix + "all"
	}
	return lifecycleRuleIDPrefix + lifecycleEscape(cleaned)
}

// lifecycleEscape hex-encodes every byte outside the set VK Cloud accepts.
//
// The marker is '_', which is inside the accepted set — '%' is not, and a
// rejected rule ID means no rule at all rather than a differently-named one.
// '_' is escaped as "_5F" like any other non-alphanumeric, so the marker can
// never be confused with a literal one. That is what makes the encoding
// injective rather than merely safe: without it, "a_2Fb" and "a/b" would both
// encode to "a_2Fb".
func lifecycleEscape(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '.':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0x0f])
		}
	}
	return b.String()
}
