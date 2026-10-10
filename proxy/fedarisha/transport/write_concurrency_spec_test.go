package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// concurrencyStore records the peak number of Upload calls in flight at once.
type concurrencyStore struct {
	*fakeStore
	hold time.Duration

	mu      sync.Mutex
	active  int
	peak    int
	blocked int
}

func (s *concurrencyStore) Upload(ctx context.Context, path string, data []byte) error {
	s.mu.Lock()
	s.active++
	if s.active > s.peak {
		s.peak = s.active
	}
	s.mu.Unlock()

	select {
	case <-time.After(s.hold):
	case <-ctx.Done():
	}

	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	return nil
}

func (s *concurrencyStore) peakConcurrency() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

func shrinkHedgeDelay(t *testing.T, d time.Duration) {
	t.Helper()
	original := uploadHedgeSmall
	uploadHedgeSmall = d
	t.Cleanup(func() { uploadHedgeSmall = original })
}

// uploadWithTimeout can have uploadAttempts copies of a file in flight at once,
// and there are uploadWorkers of them, so one connection can reach
// 32 × 3 = 96 concurrent PUTs. The write pool is 48 connections.
//
// The excess does not fail — it waits in the transport's dial queue, and that
// waiting is charged against the upload budget. So under pressure every PUT
// spends part of its allowance standing in a queue, which trips the hedge
// timer, which adds more PUTs to the same queue. The hedging mechanism turns
// into "triplicate every PUT" precisely when the pool is under pressure, which
// is the self-inflicted stampede the read side was already fixed for with
// readSem.
//
// Small payloads use the short hedge delay, so a test does not have to wait
// three seconds to watch it happen.
func TestHedgingDoesNotExceedTheWritePool(t *testing.T) {
	shrinkHedgeDelay(t, 30*time.Millisecond)

	store := &concurrencyStore{fakeStore: newFakeStore(), hold: 150 * time.Millisecond}

	conn := NewConn(ConnConfig{
		Store:       store,
		SessionID:   GenerateSessionID(),
		SessionDir:  "sessions/conc",
		MaxFileSize: 512,
	})
	defer conn.Close()

	// Enough small writes to keep every worker busy for several hedge windows.
	for i := 0; i < 400; i++ {
		if _, err := conn.Write(make([]byte, 64)); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(4 * time.Second)
	peak := 0
	for time.Now().Before(deadline) {
		if p := store.peakConcurrency(); p > peak {
			peak = p
			if p > uploadWorkers {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	if peak > uploadWorkers {
		t.Errorf("peak concurrent uploads was %d, want at most %d: hedging multiplies "+
			"a file into %d copies across %d workers against a pool of 48 connections",
			peak, uploadWorkers, uploadAttempts, uploadWorkers)
	}
}

var _ storage.Storage = (*fakeStore)(nil)
