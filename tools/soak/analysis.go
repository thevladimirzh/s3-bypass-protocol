package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Transfer is one completed (or failed) request through the tunnel.
//
// Recorded per transfer rather than aggregated on the fly, because the whole
// point of a soak is the shape of what happened: a tunnel that averages fine
// but stalls for 20 seconds every few minutes is broken in exactly the way a
// mean-throughput number hides.
type Transfer struct {
	Worker   int           `json:"worker"`
	Start    time.Time     `json:"start"`
	Duration time.Duration `json:"duration"`
	TTFB     time.Duration `json:"ttfb"`
	Bytes    int64         `json:"bytes"`
	Dir      string        `json:"dir"` // "up" or "down"
	Err      string        `json:"err,omitempty"`
}

// Thresholds for the two directions. They are different numbers measuring
// different things, which is the whole reason they are not one flag.
//
// Downloads are judged on time-to-first-byte. Nothing legitimate makes the
// first byte of a response arrive seconds late — that is the symptom itself.
//
// Uploads cannot use time-to-first-byte at all. A response's headers arrive
// only after the request body has been sent in full, so "TTFB" on an upload is
// really "how long the body took", and at a realistic 0.8 MB/s a 4MB upload
// crosses a 5-second line every single time. Using one threshold for both
// directions reports a healthy tunnel as stalling on every second request,
// which is how a measuring tool loses the room to be believed.
type Thresholds struct {
	DownTTFB   time.Duration
	UpDuration time.Duration
}

// Stalled reports whether this transfer is evidence of the symptom.
func (t Transfer) Stalled(th Thresholds) bool {
	if t.Err != "" {
		return true
	}
	if t.Dir == "up" {
		return t.Duration >= th.UpDuration
	}
	return t.TTFB >= th.DownTTFB
}

// Explain names the number a stall was judged on, so a report cannot be read
// back with the wrong metric in mind.
func (t Transfer) Explain(th Thresholds) string {
	if t.Err != "" {
		return "error: " + t.Err
	}
	if t.Dir == "up" {
		return fmt.Sprintf("upload took %s (threshold %s)", t.Duration.Round(time.Millisecond), th.UpDuration)
	}
	return fmt.Sprintf("first byte after %s (threshold %s)", t.TTFB.Round(time.Millisecond), th.DownTTFB)
}

// Bucket is one slice of the run, used to make a stall visible as a hole in the
// timeline rather than a number inside an average.
type Bucket struct {
	Start      time.Time     `json:"start"`
	Transfers  int           `json:"transfers"`
	Bytes      int64         `json:"bytes"`
	Stalls     int           `json:"stalls"`
	MaxTTFB    time.Duration `json:"maxTTFB"`
	Throughput float64       `json:"throughput"` // bytes/sec over the bucket
}

// Bucketize splits transfers into fixed-width slices.
//
// Bucket width is chosen by the caller, not derived: the caller knows whether
// it is watching for a 20-second hitch or a 5-minute outage, and a tool that
// picks its own resolution hides exactly the thing it was pointed at.
func Bucketize(transfers []Transfer, start time.Time, width time.Duration, th Thresholds) []Bucket {
	if width <= 0 {
		return nil
	}

	// Index by slice number, holding the POSITION in the output — not a pointer
	// to a Bucket. The first version of this appended a zero value and then
	// tried to find it again by timestamp, so every increment landed in a
	// detached copy and the timeline came out entirely empty. Holding an index
	// removes the step where that can go wrong.
	index := make(map[int64]int)
	var out []Bucket

	for _, t := range transfers {
		if t.Start.Before(start) {
			continue
		}
		slot := int64(t.Start.Sub(start) / width)

		i, ok := index[slot]
		if !ok {
			i = len(out)
			index[slot] = i
			out = append(out, Bucket{Start: start.Add(time.Duration(slot) * width)})
		}

		out[i].Transfers++
		out[i].Bytes += t.Bytes
		if t.Stalled(th) {
			out[i].Stalls++
		}
		if t.TTFB > out[i].MaxTTFB {
			out[i].MaxTTFB = t.TTFB
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	for i := range out {
		out[i].Throughput = float64(out[i].Bytes) / width.Seconds()
	}
	return out
}

// Summary is what a soak actually reports.
type Summary struct {
	Started    time.Time     `json:"started"`
	Duration   time.Duration `json:"duration"`
	Transfers  int           `json:"transfers"`
	Bytes      int64         `json:"bytes"`
	Failures   int           `json:"failures"`
	Stalls     int           `json:"stalls"`
	StallsDown int           `json:"stallsDown"`
	StallsUp   int           `json:"stallsUp"`
	WorstTTFB  time.Duration `json:"worstTTFB"`
	MedianTTFB time.Duration `json:"medianTTFB"`
	Throughput float64       `json:"throughput"` // bytes/sec across the whole run
}

func summarize(transfers []Transfer, start, end time.Time, th Thresholds) Summary {
	s := Summary{Started: start, Duration: end.Sub(start)}
	ttfbs := make([]time.Duration, 0, len(transfers))
	for _, t := range transfers {
		s.Transfers++
		s.Bytes += t.Bytes
		if t.Err != "" {
			s.Failures++
		}
		if t.Stalled(th) {
			s.Stalls++
			if t.Dir == "up" {
				s.StallsUp++
			} else {
				s.StallsDown++
			}
		}
		if t.TTFB > s.WorstTTFB {
			s.WorstTTFB = t.TTFB
		}
		if t.Err == "" {
			ttfbs = append(ttfbs, t.TTFB)
		}
	}
	sort.Slice(ttfbs, func(i, j int) bool { return ttfbs[i] < ttfbs[j] })
	if len(ttfbs) > 0 {
		s.MedianTTFB = ttfbs[len(ttfbs)/2]
	}
	if el := s.Duration.Seconds(); el > 0 {
		s.Throughput = float64(s.Bytes) / el
	}
	return s
}

// --- log correlation ---------------------------------------------------------
//
// This is the part that makes the soak worth writing. A stall on its own says
// "something stopped"; a stall sitting next to "read list failed" in the
// tunnel log says the backend stopped answering, which is a different bug to go
// and fix. Before the M2/M6 fixes those lines did not exist at all — the
// backend failing was indistinguishable from nothing happening.

// diagnosticMarkers are the log lines this fork added around the bugs the soak
// is looking for. Matching them turns a mystery stall into an attributed one.
var diagnosticMarkers = []string{
	"read list failed",
	"delete queue full",
	"cleanup:",
	"keepalive failed",
	"read GET failed",
	"slow down",
	"timeout",
}

// LogEntry is one parsed line from a tunnel log.
type LogEntry struct {
	Time   time.Time
	Line   string
	Source string
}

// LoadLog parses the timestamped lines a fedarisha log is made of. Lines it
// cannot parse are dropped rather than guessed at: a wrong timestamp would
// place a stall next to an unrelated error and make the correlation worse than
// no correlation at all.
func LoadLog(path string) ([]LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []LogEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		raw := sc.Text()
		ts, rest, ok := splitTimestamp(raw)
		if !ok {
			continue
		}
		out = append(out, LogEntry{Time: ts, Line: rest, Source: path})
	}
	return out, sc.Err()
}

// splitTimestamp pulls a leading "2006/01/02 15:04:05.000000" off a log line.
func splitTimestamp(line string) (time.Time, string, bool) {
	const layout = "2006/01/02 15:04:05.000000"
	if len(line) < len(layout)+1 {
		return time.Time{}, "", false
	}
	ts, err := time.ParseInLocation(layout, line[:len(layout)], time.Local)
	if err != nil {
		return time.Time{}, "", false
	}
	return ts, strings.TrimSpace(line[len(layout):]), true
}

// Attribution is what the soak reports about one stall.
type Attribution struct {
	At     time.Time     `json:"at"`
	Window time.Duration `json:"-"`
	Lines  []string      `json:"lines"`
}

// Attribute finds the log lines near a stall that explain it.
//
// The window is deliberately generous in both directions: a backend that
// starts timing out announces it in a burst, and the transfer that actually
// suffers may be a second or two away from the first line about it. Matching
// only lines after the stall would miss the cause and blame the symptom.
func Attribute(entries []LogEntry, at time.Time, window time.Duration) Attribution {
	a := Attribution{At: at, Window: window}
	for _, e := range entries {
		d := e.Time.Sub(at)
		if d < -window || d > window {
			continue
		}
		lower := strings.ToLower(e.Line)
		for _, m := range diagnosticMarkers {
			if strings.Contains(lower, strings.ToLower(m)) {
				a.Lines = append(a.Lines, e.Line)
				break
			}
		}
	}
	return a
}
