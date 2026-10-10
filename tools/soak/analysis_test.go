package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- what counts as a stall --------------------------------------------------

var testThresholds = Thresholds{DownTTFB: 5 * time.Second, UpDuration: 15 * time.Second}

// Spec: the symptom is a tunnel that stops, not one that is slow. A large
// transfer legitimately runs long; nothing legitimate makes the FIRST byte
// arrive seconds late. Measuring the wrong half of the number would have
// reported a healthy 10MB download as broken.
func TestSlowDownloadIsNotAStallButALateFirstByteIs(t *testing.T) {
	slow := Transfer{Dir: "down", TTFB: 80 * time.Millisecond, Duration: 40 * time.Second, Bytes: 10 << 20}
	if slow.Stalled(testThresholds) {
		t.Fatal("a download that took 40s to finish but sent its first byte in 80ms is not stalled")
	}

	late := Transfer{Dir: "down", TTFB: 20 * time.Second, Duration: 41 * time.Second, Bytes: 8 << 20}
	if !late.Stalled(testThresholds) {
		t.Fatal("20s before the first byte is the exact symptom this tool exists to catch")
	}
}

// Spec: uploads must NOT be judged on time-to-first-byte. A response's headers
// only arrive after the request body has been sent in full, so TTFB on an
// upload is really "how long the body took" — at a realistic ~0.8 MB/s a 4MB
// upload crosses a 5-second line every single time.
//
// This is not hypothetical: the first live run reported two stalls in 65
// seconds, both of them uploads of 5.4s that were entirely healthy.
func TestAHealthyUploadIsNotAStall(t *testing.T) {
	// 4MB at roughly 0.75 MB/s — an ordinary upload through the tunnel.
	ordinary := Transfer{Dir: "up", TTFB: 5 * time.Second, Duration: 5 * time.Second, Bytes: 0}
	if ordinary.Stalled(Thresholds{DownTTFB: 5 * time.Second, UpDuration: 15 * time.Second}) {
		t.Fatal("a 5.4s upload is not a stall; judging uploads by time-to-first-byte reports a healthy tunnel as broken")
	}

	// Even one that trips the DOWNLOAD threshold must not be judged by it.
	if ordinary.Stalled(Thresholds{DownTTFB: 4 * time.Second, UpDuration: 30 * time.Second}) {
		t.Fatal("the download threshold must never be applied to an upload")
	}

	// An upload that really is stuck still has to be caught.
	stuck := Transfer{Dir: "up", Duration: 90 * time.Second, Bytes: 0}
	if !stuck.Stalled(testThresholds) {
		t.Fatal("a 90-second upload is a stall and must be reported")
	}
}

func TestFailedTransferIsAlwaysAStall(t *testing.T) {
	// A zero TTFB with an error must not slip through as a clean fast transfer.
	broken := Transfer{Dir: "down", TTFB: 0, Err: "context deadline exceeded"}
	if !broken.Stalled(testThresholds) {
		t.Fatal("a failed transfer is a stall regardless of its TTFB")
	}
}

func TestExplainNamesTheMetricItJudged(t *testing.T) {
	up := Transfer{Dir: "up", Duration: 40 * time.Second, TTFB: 40 * time.Second}
	if got := up.Explain(testThresholds); !strings.Contains(got, "upload took") {
		t.Fatalf("an upload stall must be explained by duration, got %q", got)
	}
	down := Transfer{Dir: "down", Duration: 41 * time.Second, TTFB: 20 * time.Second}
	if got := down.Explain(testThresholds); !strings.Contains(got, "first byte") {
		t.Fatalf("a download stall must be explained by first-byte time, got %q", got)
	}
}

// --- the timeline ------------------------------------------------------------

// Spec: a stall has to be visible as a hole in the shape of the run, which
// means buckets must actually separate transfers rather than lumping them.
func TestBucketizeSeparatesTransfersInTime(t *testing.T) {
	start := time.Date(2026, 10, 11, 14, 0, 0, 0, time.Local)
	var ts []Transfer
	// Three transfers inside the first 30s slice.
	for i := 0; i < 3; i++ {
		ts = append(ts, Transfer{Dir: "down", Start: start.Add(time.Duration(i) * time.Second), Bytes: 100, TTFB: 10 * time.Millisecond})
	}
	// One, stalled, in the second slice.
	ts = append(ts, Transfer{Dir: "down", Start: start.Add(45 * time.Second), Bytes: 100, TTFB: 22 * time.Second})

	buckets := Bucketize(ts, start, 30*time.Second, testThresholds)
	if len(buckets) != 2 {
		t.Fatalf("want 2 slices, got %d", len(buckets))
	}
	if buckets[0].Transfers != 3 || buckets[0].Stalls != 0 {
		t.Fatalf("slice 0: %d transfers, %d stalls; want 3 and 0", buckets[0].Transfers, buckets[0].Stalls)
	}
	if buckets[1].Transfers != 1 || buckets[1].Stalls != 1 {
		t.Fatalf("slice 1: %d transfers, %d stalls; want 1 and 1", buckets[1].Transfers, buckets[1].Stalls)
	}
	if buckets[0].Start.Sub(start) != 0 || buckets[1].Start.Sub(start) != 30*time.Second {
		t.Fatalf("slices are not aligned to the requested width: %v, %v", buckets[0].Start, buckets[1].Start)
	}
}

func TestSummarizeCountsStallsFailuresAndBytes(t *testing.T) {
	start := time.Now()
	ts := []Transfer{
		{Dir: "down", Start: start, TTFB: 10 * time.Millisecond, Bytes: 1000, Duration: time.Second},
		{Dir: "down", Start: start, TTFB: 20 * time.Millisecond, Bytes: 2000, Duration: time.Second},
		{Dir: "down", Start: start, TTFB: 30 * time.Second, Bytes: 500, Duration: 31 * time.Second},
		{Dir: "down", Start: start, TTFB: 0, Bytes: 0, Err: "connection reset"},
	}
	s := summarize(ts, start, start.Add(10*time.Second), testThresholds)

	if s.Transfers != 4 {
		t.Fatalf("transfers = %d, want 4", s.Transfers)
	}
	if s.Failures != 1 {
		t.Fatalf("failures = %d, want 1", s.Failures)
	}
	if s.Stalls != 2 {
		t.Fatalf("stalls = %d, want 2 (the late one and the failure)", s.Stalls)
	}
	if s.Bytes != 3500 {
		t.Fatalf("bytes = %d, want 3500", s.Bytes)
	}
	if s.WorstTTFB != 30*time.Second {
		t.Fatalf("worst ttfb = %v, want 30s", s.WorstTTFB)
	}
	// Median over the three that succeeded.
	if s.MedianTTFB != 20*time.Millisecond {
		t.Fatalf("median ttfb = %v, want 20ms (failures must not drag it to zero)", s.MedianTTFB)
	}
}

// --- log correlation ---------------------------------------------------------

func TestSplitTimestamp(t *testing.T) {
	line := "2026/10/11 14:32:10.123456 [fedarisha:abc] read list failed (retrying): context deadline exceeded"
	ts, rest, ok := splitTimestamp(line)
	if !ok {
		t.Fatal("a well-formed log line must parse")
	}
	if ts.Hour() != 14 || ts.Minute() != 32 || ts.Second() != 10 {
		t.Fatalf("timestamp parsed wrong: %v", ts)
	}
	if !strings.Contains(rest, "read list failed") {
		t.Fatalf("remainder lost the message: %q", rest)
	}

	// A line with no parseable timestamp must be REJECTED, not guessed at: a
	// wrong timestamp would place a stall beside an unrelated error and make
	// the correlation worse than none at all.
	if _, _, ok := splitTimestamp("not a log line"); ok {
		t.Fatal("an unparseable line must not be accepted")
	}
}

// Spec: a stall is attributable when the tunnel said something was wrong near
// it. This is the whole reason the tool reads the log — before the M2/M6 fixes
// those lines did not exist, so a backend failing was indistinguishable from
// nothing happening.
func TestAttributeFindsTheBackendFailureAroundAStall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "client.log")
	body := strings.Join([]string{
		"2026/10/11 14:31:40.000000 [Info] something ordinary",
		"2026/10/11 14:31:58.000000 [fedarisha:abc] read list failed (retrying): context deadline exceeded",
		"2026/10/11 14:32:10.000000 [fedarisha:abc] yamux: keepalive failed: i/o deadline reached",
		"2026/10/11 14:33:00.000000 [Info] far away, unrelated",
		"garbage line that must be ignored",
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	entries, err := LoadLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("loaded %d entries, want 4 (the garbage line must be dropped)", len(entries))
	}

	at := time.Date(2026, 10, 11, 14, 32, 10, 0, time.Local)
	a := Attribute(entries, at, 15*time.Second)
	if len(a.Lines) != 2 {
		t.Fatalf("attributed %d lines, want 2:\n%s", len(a.Lines), strings.Join(a.Lines, "\n"))
	}
	joined := strings.Join(a.Lines, "\n")
	if !strings.Contains(joined, "read list failed") {
		t.Error("the list failure 12s before the stall must be attributed to it")
	}
	if !strings.Contains(joined, "keepalive failed") {
		t.Error("the keepalive failure at the stall must be attributed")
	}
	if strings.Contains(joined, "far away") {
		t.Error("a line outside the window must not be attributed")
	}
}

// Spec: a clean window must produce an honest "nothing found" rather than
// blaming whatever line happened to be nearest.
func TestAttributeReportsNothingWhenTheWindowIsClean(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "quiet.log")
	body := "2026/10/11 14:00:00.000000 [Info] nothing interesting here\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := LoadLog(path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 11, 14, 32, 10, 0, time.Local)
	if a := Attribute(entries, at, 10*time.Second); len(a.Lines) != 0 {
		t.Fatalf("attributed %d unrelated lines: %v", len(a.Lines), a.Lines)
	}
}
