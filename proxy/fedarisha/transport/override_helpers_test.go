package transport

import (
	"testing"
	"time"
)

// Test-only overrides, next to the specs that need them so the reason each
// exists is visible where it is used and production code keeps plain
// constants wherever a constant will do.
func shrinkUploadTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	original := uploadTimeout
	uploadTimeout = d
	t.Cleanup(func() { uploadTimeout = original })
}

func shrinkHoleTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	original := holeTimeout
	holeTimeout = d
	t.Cleanup(func() { holeTimeout = original })
}
