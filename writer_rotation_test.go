package zlog

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testClock is a race free fake clock: the writer's background flusher reads
// the same clock the test advances.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock(t time.Time) *testClock {
	return &testClock{now: t}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *testClock) Advance(d time.Duration) {
	c.Set(c.Now().Add(d))
}

// Hourly rotation must keep every entry in the file named after the hour it
// was written in, including the entries written across a day boundary.
func TestFileWriterKeepsEntriesInMatchingHourFile(t *testing.T) {
	dir := t.TempDir()
	clock := newTestClock(time.Date(2026, 9, 15, 22, 59, 0, 0, time.Local))
	w, err := newFileWriter(writerConfig{dir: dir, name: "app"}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	want := map[string][]string{}
	for i := 0; i < 4*60; i++ { // four hours of one entry per minute
		clock.Advance(time.Minute)
		line := clock.Now().Format("2006-01-02-15-04") + "\n"
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		hour := clock.Now().Format("2006-01-02-15")
		want[hour] = append(want[hour], line)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name())
	}
	t.Logf("files: %v", got)

	for hour, lines := range want {
		b, err := os.ReadFile(filepath.Join(dir, "app-"+hour+".log"))
		if err != nil {
			t.Fatalf("hour %s: %v", hour, err)
		}
		if string(b) != strings.Join(lines, "") {
			t.Fatalf("hour %s holds %d entries, want %d\ngot tail: %q",
				hour, strings.Count(string(b), "\n"), len(lines), lastLines(string(b), 3))
		}
	}
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// A restart right after midnight must not lose entries and must not write
// into the previous day's file.
func TestFileWriterRestartAfterMidnight(t *testing.T) {
	dir := t.TempDir()

	before := newTestClock(time.Date(2026, 9, 15, 23, 55, 0, 0, time.Local))
	w1, err := newFileWriter(writerConfig{dir: dir, name: "app"}, before.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.Write([]byte("before-midnight\n")); err != nil {
		t.Fatal(err)
	}
	if err := w1.Close(); err != nil {
		t.Fatal(err)
	}

	after := newTestClock(time.Date(2026, 9, 16, 0, 5, 0, 0, time.Local))
	w2, err := newFileWriter(writerConfig{dir: dir, name: "app"}, after.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Write([]byte("after-midnight\n")); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	if got := readFile(t, filepath.Join(dir, "app-2026-09-16-00.log")); got != "after-midnight\n" {
		t.Fatalf("post-restart file: %q", got)
	}
	if got := readFile(t, filepath.Join(dir, "app-2026-09-15-23.log")); got != "before-midnight\n" {
		t.Fatalf("pre-midnight file: %q", got)
	}
}

// A clock stepped backwards (NTP on a board without an RTC) must not leave
// the writer appending to a file whose name no longer matches the clock.
func TestFileWriterClockSteppedBackwards(t *testing.T) {
	dir := t.TempDir()
	clock := newTestClock(time.Date(2026, 9, 16, 0, 30, 0, 0, time.Local))
	w, err := newFileWriter(writerConfig{dir: dir, name: "app"}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	write := func(s string) {
		t.Helper()
		if _, err := w.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}

	write("at-0030\n")
	clock.Set(time.Date(2026, 9, 15, 21, 10, 0, 0, time.Local))
	write("after-step-back\n")

	if got := readFile(t, filepath.Join(dir, "app-2026-09-15-21.log")); got != "after-step-back\n" {
		t.Fatalf("entry after a backward clock step went to the wrong file: %q", got)
	}
}

// A failing write (a full disk, for example) must not silently discard
// buffered entries.
func TestFileWriterKeepsBufferedDataOnWriteError(t *testing.T) {
	dir := t.TempDir()
	clock := newTestClock(time.Date(2026, 9, 16, 1, 0, 0, 0, time.Local))
	w, err := newFileWriter(writerConfig{dir: dir, name: "app"}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.mu.Lock()
	path := w.file.Name()
	good := w.file
	w.mu.Unlock()

	// a read-only handle makes every write fail, like ENOSPC would
	ro, err := os.OpenFile(path, os.O_RDONLY, fileMode)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	w.mu.Lock()
	w.file = ro
	w.mu.Unlock()

	if _, err := w.Write([]byte("must-survive\n")); err != nil {
		t.Fatalf("buffering an entry must not fail: %v", err)
	}
	if err := w.Sync(); err == nil {
		t.Fatal("expected the flush to fail against a read-only handle")
	}

	w.mu.Lock()
	w.file = good
	w.mu.Unlock()

	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); !strings.Contains(got, "must-survive\n") {
		t.Fatalf("buffered entry was dropped after a write error: %q", got)
	}
}

// A rotation failure must not drop the entry that triggered it.
func TestFileWriterRotationFailureKeepsEntries(t *testing.T) {
	dir := t.TempDir()
	clock := newTestClock(time.Date(2026, 9, 16, 0, 59, 0, 0, time.Local))
	w, err := newFileWriter(writerConfig{dir: dir, name: "app"}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// break the log path so the next rotation cannot create the new file
	moved := dir + "_moved"
	if err := os.Rename(dir, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, []byte("block"), 0o644); err != nil {
		t.Fatal(err)
	}

	clock.Set(time.Date(2026, 9, 16, 1, 0, 30, 0, time.Local))
	if _, err := w.Write([]byte("during-failed-rotation\n")); err != nil {
		t.Fatalf("write must keep flowing while rotation is broken: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync must keep working while rotation is broken: %v", err)
	}

	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(moved, dir); err != nil {
		t.Fatal(err)
	}
	clock.Set(time.Date(2026, 9, 16, 1, 5, 0, 0, time.Local))
	if _, err := w.Write([]byte("after-recovery\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "app-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	var all string
	for _, f := range files {
		all += readFile(t, f)
	}
	for _, want := range []string{"during-failed-rotation\n", "after-recovery\n"} {
		if !strings.Contains(all, want) {
			t.Fatalf("entry %q was lost across a failed rotation; files hold %q", want, all)
		}
	}
}

// The live log link must resolve even when Path is relative (go-zero's
// default), otherwise tailing it silently fails.
func TestFileWriterLinkResolves(t *testing.T) {
	base := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(base); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(cwd) }()

	clock := newTestClock(time.Date(2026, 9, 16, 2, 0, 0, 0, time.Local))
	w, err := newFileWriter(writerConfig{dir: filepath.Join("logs", "app"), name: "app"}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := w.Write([]byte("live\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	if _, err := w.Write([]byte("rotated\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	link := filepath.Join("logs", "app", "app.log")
	target, err := os.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(target) {
		t.Fatalf("link target %q must be absolute", target)
	}
	if got := readFile(t, link); got != "rotated\n" {
		t.Fatalf("reading through the live link returned %q", got)
	}
}

// Retention must not delete a sibling logger's files that merely share a
// name prefix ("app" vs "app-errors" writing into the same directory).
func TestFileWriterCleanupLeavesSiblingFilesAlone(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.Local)
	own := filepath.Join(dir, "app-2026-08-18-10.log")
	sibling := filepath.Join(dir, "app-errors-2026-08-18-10.log")
	for _, p := range []string{own, sibling} {
		if err := os.WriteFile(p, []byte("x"), fileMode); err != nil {
			t.Fatal(err)
		}
		old := now.Add(-720 * time.Hour)
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}

	w, err := newFileWriter(writerConfig{dir: dir, name: "app", keepDays: 1}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := os.Stat(own); !os.IsNotExist(err) {
		t.Fatalf("expired file of this logger should be removed, stat err = %v", err)
	}
	if _, err := os.Stat(sibling); err != nil {
		t.Fatalf("sibling logger file must be left alone: %v", err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
