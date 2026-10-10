package main

// soak drives sustained traffic through a fedarisha tunnel and records every
// stall it sees.
//
// Why this exists: the symptom this fork was built to fix — a session that
// stops dead for 15-29 seconds — was only ever observed by hand, with curl, on
// a short session. That is not enough to tell whether it is gone. A soak makes
// the symptom measurable over the only conditions that produce it: a long
// session, under load, with the backend quietly timing out in the middle.
//
// It exercises BOTH directions on purpose. The original defect was in the
// writer — a PUT dropped after uploadAttempts — so a download-only test would
// miss precisely the bug this work exists for. An upload and a download per
// round, alternating, keeps both paths under pressure.
//
// On a stall it pulls the tunnel's own log lines from the surrounding window
// and prints them next to the stall. That is what turns "something stopped at
// 14:32" into "the backend stopped answering at 14:31:58", which are different
// bugs to go and chase.
//
//	go run ./tools/soak -proxy socks5://127.0.0.1:11080 -duration 20m
//	go run ./tools/soak -duration 2m -size 2MB -concurrency 4 -stall 5s
//	go run ./tools/soak -logs client.log -report soak.json

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// sizeFlag accepts "4MB" as readily as 4194304. A tool you point at a tunnel
// is a tool someone types into by hand, and the answer to "how big" is never
// written as a raw byte count.
type sizeFlag int64

func (s *sizeFlag) String() string { return humanBytes(int64(*s)) }

func (s *sizeFlag) Set(v string) error {
	n, err := parseSize(v)
	if err != nil {
		return err
	}
	*s = sizeFlag(n)
	return nil
}

func parseSize(v string) (int64, error) {
	v = strings.TrimSpace(strings.ToUpper(v))
	mult := int64(1)
	switch {
	case strings.HasSuffix(v, "KB"):
		mult, v = 1<<10, strings.TrimSuffix(v, "KB")
	case strings.HasSuffix(v, "MB"):
		mult, v = 1<<20, strings.TrimSuffix(v, "MB")
	case strings.HasSuffix(v, "GB"):
		mult, v = 1<<30, strings.TrimSuffix(v, "GB")
	case strings.HasSuffix(v, "B"):
		v = strings.TrimSuffix(v, "B")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot read size %q: try 4MB", v)
	}
	return n * mult, nil
}

var (
	proxyAddr            = flag.String("proxy", "socks5://127.0.0.1:11080", "SOCKS5 proxy to drive traffic through")
	duration             = flag.Duration("duration", 10*time.Minute, "how long to keep traffic flowing")
	concurrency          = flag.Int("concurrency", 4, "parallel workers")
	stallDown            = flag.Duration("stall", 5*time.Second, "download stall: time-to-first-byte at or above this")
	stallUp              = flag.Duration("stall-up", 0, "upload stall: total duration at or above this (0 = 3x -stall)")
	reqTimeout           = flag.Duration("timeout", 60*time.Second, "hard per-request ceiling")
	downURL              = flag.String("url-down", "https://speed.cloudflare.com/__down", "download endpoint (bytes query appended)")
	upURL                = flag.String("url-up", "https://speed.cloudflare.com/__up", "upload endpoint")
	reportPath           = flag.String("report", "", "write the full JSON report here")
	logPaths             = flag.String("logs", "", "comma-separated tunnel log files to correlate stalls against")
	bucketWidth          = flag.Duration("bucket", 30*time.Second, "timeline resolution")
	quiet                = flag.Bool("quiet", false, "only print the summary and any stalls")
	size        sizeFlag = 4 << 20
)

func init() {
	flag.Var(&size, "size", "bytes per transfer (accepts KB/MB/GB)")
}

func main() {
	flag.Parse()

	th := Thresholds{DownTTFB: *stallDown}
	th.UpDuration = *stallUp
	if th.UpDuration == 0 {
		// Deliberately looser than the download threshold. An upload's duration
		// includes sending the body, so it scales with -size; this default suits
		// small transfers and must be raised alongside the size.
		th.UpDuration = 3 * *stallDown
	}

	proxyURL, err := url.Parse(*proxyAddr)
	if err != nil || proxyURL.Host == "" {
		fatalf("bad -proxy %q", *proxyAddr)
	}

	var logs []LogEntry
	for _, p := range strings.Split(*logPaths, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		entries, err := LoadLog(p)
		if err != nil {
			// A missing log is not fatal — the soak still measures stalls, it
			// just cannot attribute them. Say so rather than pretending.
			fmt.Fprintf(os.Stderr, "warn: cannot read log %s: %v\n", p, err)
			continue
		}
		logs = append(logs, entries...)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyURL(proxyURL),
			DisableKeepAlives:   false,
			MaxIdleConnsPerHost: *concurrency * 2,
			TLSHandshakeTimeout: 15 * time.Second,
		},
		Timeout: *reqTimeout,
	}

	var (
		mu         sync.Mutex
		all        []Transfer
		totalBytes atomic.Int64
		completed  atomic.Int64
	)

	start := time.Now()
	deadline := start.Add(*duration)

	if !*quiet {
		fmt.Printf("soak: %s for %s, %d workers x %s per transfer\n",
			*proxyAddr, *duration, *concurrency, humanBytes(int64(size)))
		fmt.Printf("      stall thresholds — download first byte %s, upload total %s\n",
			th.DownTTFB, th.UpDuration)
		if len(logs) > 0 {
			fmt.Printf("      correlating against %d log line(s)\n", len(logs))
		}
		fmt.Println()
	}

	// Heartbeat. A 20-minute run that prints nothing until the end leaves the
	// person who started it with nothing to look at and no way to tell a slow
	// run from a hung one. One line per bucket: elapsed, transfers, stalls.
	stopBeat := make(chan struct{})
	var beats sync.WaitGroup
	beats.Add(1)
	go func() {
		defer beats.Done()
		tick := time.NewTicker(*bucketWidth)
		defer tick.Stop()
		begin := time.Now()
		for {
			select {
			case <-stopBeat:
				return
			case now := <-tick.C:
				mu.Lock()
				n, stalls := len(all), 0
				for _, t := range all {
					if t.Stalled(th) {
						stalls++
					}
				}
				mu.Unlock()
				fmt.Printf("  … %s elapsed, %d transfers, %d stalled, %s moved\n",
					now.Sub(begin).Round(time.Second), n, stalls,
					humanBytes(totalBytes.Load()))
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; time.Now().Before(deadline); round++ {
				dir := "down"
				if round%2 == 1 {
					dir = "up"
				}
				t := doTransfer(client, worker, dir)

				mu.Lock()
				all = append(all, t)
				mu.Unlock()
				totalBytes.Add(t.Bytes)
				if t.Err == "" {
					completed.Add(1)
				}

				if t.Stalled(th) && !*quiet {
					reportStall(t, logs, th)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stopBeat)
	beats.Wait()
	end := time.Now()

	s := summarize(all, start, end, th)
	buckets := Bucketize(all, start, *bucketWidth, th)

	printSummary(s, buckets, *quiet)

	if *reportPath != "" {
		writeReport(*reportPath, s, buckets, all)
	}
}

// doTransfer runs one request and measures the two things that matter: how long
// the FIRST byte took, and how much actually arrived.
func doTransfer(client *http.Client, worker int, dir string) Transfer {
	t := Transfer{Worker: worker, Start: time.Now(), Dir: dir}

	var firstByte time.Time
	trace := &httptrace.ClientTrace{
		GotFirstResponseByte: func() { firstByte = time.Now() },
	}

	var req *http.Request
	var err error

	switch dir {
	case "up":
		body := make([]byte, int64(size))
		// Random, so nothing in the tunnel path can be served from a cache or
		// deduplicated by an intermediary. Zeros would make an accidental
		// short write look like a success.
		if _, err = rand.Read(body); err != nil {
			fatalf("cannot generate upload body: %v", err)
		}
		req, err = http.NewRequest(http.MethodPost, *upURL, strings.NewReader(string(body)))
	case "down":
		u := fmt.Sprintf("%s?bytes=%d", *downURL, int64(size))
		req, err = http.NewRequest(http.MethodGet, u, nil)
	}
	if err != nil {
		t.Err = err.Error()
		t.Duration = time.Since(t.Start)
		return t
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := client.Do(req)
	if err != nil {
		t.Err = err.Error()
		t.Duration = time.Since(t.Start)
		return t
	}
	defer resp.Body.Close()

	n, _ := io.Copy(io.Discard, resp.Body)
	finished := time.Now()

	t.Bytes = n
	t.Duration = finished.Sub(t.Start)
	if !firstByte.IsZero() {
		t.TTFB = firstByte.Sub(t.Start)
	} else {
		// A response with no body still has headers; without this a successful
		// empty transfer would report TTFB 0 and look impossibly fast.
		t.TTFB = t.Duration
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Err = fmt.Sprintf("status %d", resp.StatusCode)
	}
	return t
}

// reportStall prints a stall as it happens, with whatever the tunnel said
// around that moment. Printed live rather than at the end because if the run
// hangs — which is the symptom — the output so far is all the evidence there
// will ever be.
func reportStall(t Transfer, logs []LogEntry, th Thresholds) {
	fmt.Printf("STALL  %s  worker %d  %s  %s  (%s)\n",
		t.Start.Format("15:04:05.000"), t.Worker, t.Dir, t.Explain(th),
		humanBytes(t.Bytes))

	a := Attribute(logs, t.Start, 10*time.Second)
	for _, l := range a.Lines {
		fmt.Printf("       ↳ %s\n", l)
	}
	if len(a.Lines) == 0 && len(logs) > 0 {
		fmt.Printf("       ↳ (no matching tunnel log line within 10s)\n")
	}
}

func printSummary(s Summary, buckets []Bucket, quietMode bool) {
	fmt.Println()
	fmt.Println("─────────────────────────────────────────────")
	fmt.Printf("transfers      %d  (%d failed, %d stalled)\n", s.Transfers, s.Failures, s.Stalls)
	fmt.Printf("volume         %s in %s\n", humanBytes(s.Bytes), s.Duration.Round(time.Second))
	fmt.Printf("throughput     %.2f MB/s sustained\n", s.Throughput/(1<<20))
	fmt.Printf("download        %d transfers, first byte p50 %s  p90 %s  worst %s\n",
		s.DownCount, s.DownMedianTTFB.Round(time.Millisecond),
		s.DownP90TTFB.Round(time.Millisecond), s.DownWorstTTFB.Round(time.Millisecond))
	fmt.Printf("upload          %d transfers, total p50 %s  worst %s\n",
		s.UpCount, s.UpMedianDur.Round(time.Millisecond), s.UpWorstDur.Round(time.Millisecond))
	fmt.Printf("stalls          %d down (first byte), %d up (total duration)\n", s.StallsDown, s.StallsUp)
	fmt.Println("─────────────────────────────────────────────")

	if !quietMode && len(buckets) > 0 {
		fmt.Println()
		fmt.Printf("timeline (%s slices)\n", bucketWidth.String())
		peak := 0.0
		for _, b := range buckets {
			if b.Throughput > peak {
				peak = b.Throughput
			}
		}
		for _, b := range buckets {
			// Scale against the run's own peak, so the shape of the line is
			// readable regardless of absolute speed.
			width := 30
			if peak > 0 {
				width = int(float64(b.Throughput) / peak * 30)
			}
			if width < 1 && b.Transfers > 0 {
				width = 1
			}
			bar := strings.Repeat("█", width)
			mark := " "
			if b.Stalls > 0 {
				mark = "!"
			}
			fmt.Printf("%s%s %-30s %7.2f MB/s  t=%d\n",
				mark, b.Start.Format("15:04:05"), bar, b.Throughput/(1<<20), b.Transfers)
		}
		fmt.Println("  ! = at least one stall in that slice")
	}

	// The verdict, stated as what it is. A run with no stalls is evidence that
	// none occurred — not proof that there are none.
	fmt.Println()
	switch {
	case s.Stalls == 0 && s.Failures == 0:
		fmt.Println("RESULT: no stalls observed. That is what was measured, not a guarantee.")
	case s.Failures > 0:
		fmt.Printf("RESULT: %d failed transfers and %d stalls — the tunnel did not hold.\n", s.Failures, s.Stalls)
	default:
		fmt.Printf("RESULT: %d stall(s), no hard failures — degraded, not down.\n", s.Stalls)
	}
}

type fullReport struct {
	Summary   Summary    `json:"summary"`
	Buckets   []Bucket   `json:"timeline"`
	Transfers []Transfer `json:"transfers"`
}

func writeReport(path string, s Summary, buckets []Bucket, transfers []Transfer) {
	blob, err := json.MarshalIndent(fullReport{Summary: s, Buckets: buckets, Transfers: transfers}, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: cannot serialise report: %v\n", err)
		return
	}
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "warn: cannot write report: %v\n", err)
	}
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
