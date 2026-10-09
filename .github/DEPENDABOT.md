# Dependabot is disabled here on purpose

The configuration file was removed.

This fork exists to carry changes that were **validated against a real
load test** — six parallel 50 MB downloads, 600 MB, zero holes, zero
teardowns. A dependency bump cannot be assumed to preserve that:

- `aws-sdk-go-v2` and its `service/s3` module are the S3 client this work
  is *about*. Silent bumps change request behaviour in exactly the code
  path the delivery guarantee depends on.
- `actions/checkout` and `actions/setup-go` are used by the release
  workflow we patched — a major bump there can break a pipeline that
  currently works.
- Every merge triggers the full multi-platform release matrix, whether or
  not the change is wanted.

So updates are deliberate: someone reads the changelog, applies the bump,
and re-runs the load test before shipping a new release tag.

To re-enable, restore a `dependabot.yml` — but expect the above to apply.
