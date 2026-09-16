package zlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// defaultBufferSize caps the in-memory buffer per logger; producers
	// block (lossless) instead of dropping entries when it is full.
	defaultBufferSize = 1 << 20 // 1MB
	// flushInterval is how often buffered entries are written to the file.
	flushInterval = 100 * time.Millisecond
	// syncInterval is how often data is fsync'd and evicted from the page
	// cache. It bounds both the crash loss window and cache memory growth.
	syncInterval = 1 * time.Second
	// syncBytesThreshold triggers an early sync+fadvise when this many
	// dirty bytes accumulate, bounding cache growth on write bursts.
	syncBytesThreshold = 64 << 20 // 64MB
	// defaultMaxSizeMB is used when Rotation=size but MaxSize is unset.
	defaultMaxSizeMB = 100
	// cleanupInterval is the minimum interval between directory scans
	// that enforce KeepDays/MaxBackups.
	cleanupInterval = time.Hour
	// errReportInterval rate-limits internal failure reports on stderr.
	// A failure such as a full disk keeps repeating, so reporting every
	// occurrence would flood stderr and reporting only the first one would
	// hide a problem that is still going on.
	errReportInterval = time.Minute

	fileMode = 0o644
	dirMode  = 0o755

	// timeHourLayout names hourly rotated files (time based rotation),
	// timeSecondLayout names size rotated files.
	timeHourLayout   = "2006-01-02-15"
	timeSecondLayout = "2006-01-02-15-04-05"
)

type writerConfig struct {
	dir      string
	name     string
	keepDays int
	// maxFiles is the maximum number of log files kept in the directory,
	// counting the file currently being written; 0 means unlimited.
	// It comes from go-zero's LogConf.MaxBackups.
	maxFiles int
	maxSize  int64 // bytes; only honored when rotation == "size"
	rotation string
}

// fileWriter is a concurrency-safe, buffered, rotating file writer.
// It is a zapcore.WriteSyncer. Every syncInterval (or syncBytesThreshold
// bytes) it fsyncs the file and drops the file's pages from the kernel
// page cache, which is what keeps the process cgroup memory from being
// inflated by log volume.
type fileWriter struct {
	cfg writerConfig

	mu          sync.Mutex
	buf         []byte
	file        *os.File
	curLen      int64 // bytes in the current file
	dirty       int64 // bytes written since the last sync+fadvise
	lastSync    time.Time
	lastCleanup time.Time
	// curSlot is the rotation slot (the truncated hour) that the open file
	// belongs to. Rotation compares it with the wall clock instead of
	// tracking a "next rotation" deadline, so a clock step in either
	// direction can never leave the writer appending to a file that does
	// not match the timestamps inside it.
	curSlot time.Time
	closed  bool
	now     func() time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	errMu         sync.Mutex // guards lastErrReport
	lastErrReport time.Time
}

func newFileWriter(cfg writerConfig, now ...func() time.Time) (*fileWriter, error) {
	if cfg.rotation == "size" && cfg.maxSize <= 0 {
		cfg.maxSize = int64(defaultMaxSizeMB) << 20
	}
	clock := time.Now
	if len(now) > 0 && now[0] != nil {
		clock = now[0]
	}
	w := &fileWriter{
		cfg:         cfg,
		buf:         make([]byte, 0, defaultBufferSize),
		now:         clock,
		lastCleanup: clock(),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	if err := w.open(); err != nil {
		return nil, err
	}
	w.cleanupLocked()
	go w.loop()
	return w, nil
}

func (w *fileWriter) loop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.mu.Lock()
			if err := w.flushLocked(); err != nil {
				w.reportErr(err)
			}
			if w.now().Sub(w.lastSync) >= syncInterval {
				if err := w.syncLocked(); err != nil {
					w.reportErr(err)
				}
			}
			if w.now().Sub(w.lastCleanup) >= cleanupInterval {
				w.lastCleanup = w.now()
				w.cleanupLocked()
			}
			w.mu.Unlock()
		}
	}
}

func (w *fileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	if len(p) > cap(w.buf) {
		// oversized entry: bypass the buffer, but drain pending data first
		if err := w.flushLocked(); err != nil {
			w.reportErr(err)
		}
		return w.writeFileLocked(p)
	}
	if len(w.buf)+len(p) > cap(w.buf) {
		if err := w.flushLocked(); err != nil {
			// The buffer could not be fully drained (a full disk, for
			// example). Keep the entry whenever there is room for it: it
			// is retried on the next flush instead of being dropped.
			w.reportErr(err)
			if len(w.buf)+len(p) > cap(w.buf) {
				return 0, err
			}
		}
	}
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *fileWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if err := w.flushLocked(); err != nil {
		return err
	}
	return w.syncLocked()
}

func (w *fileWriter) Close() error {
	w.stopOnce.Do(func() { close(w.stopCh) })
	<-w.doneCh
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.flushLocked()
	_ = w.syncLocked()
	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

// flushLocked writes the buffered entries out. Bytes that could not be
// written (a short write or an error such as ENOSPC) stay in the buffer so
// the caller can retry them, which is what keeps a failing disk from
// silently discarding logs.
func (w *fileWriter) flushLocked() error {
	for len(w.buf) > 0 {
		n, err := w.writeFileLocked(w.buf)
		if n > 0 {
			w.buf = append(w.buf[:0], w.buf[n:]...)
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (w *fileWriter) writeFileLocked(p []byte) (int, error) {
	if w.file == nil {
		// self-heal: a previous failed rotation must not kill the logger
		if err := w.open(); err != nil {
			w.reportErr(err)
			return 0, err
		}
	}
	if w.shouldRotateLocked() {
		if err := w.rotateLocked(); err != nil {
			// Keep appending to the current file rather than dropping the
			// entry; the rotation is retried on the next write.
			w.reportErr(err)
		}
	}
	n, err := w.file.Write(p)
	w.curLen += int64(n)
	w.dirty += int64(n)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return n, err
	}
	if w.cfg.rotation == "size" && w.cfg.maxSize > 0 && w.curLen >= w.cfg.maxSize {
		if rerr := w.rotateLocked(); rerr != nil {
			w.reportErr(rerr)
		}
	}
	if w.dirty >= syncBytesThreshold {
		// a failed fsync means the data is not durable yet, not that it was
		// not written: report it and keep going
		if serr := w.syncLocked(); serr != nil {
			w.reportErr(serr)
		}
	}
	return n, nil
}

// shouldRotateLocked reports whether the open file still matches the
// current rotation slot. Comparing slots (rather than a "next rotation"
// deadline) also covers the boundary instant itself and clock steps: an
// entry written at exactly HH:00:00 belongs to the HH file, and after a
// backward step the writer returns to the file matching the wall clock.
func (w *fileWriter) shouldRotateLocked() bool {
	if w.cfg.rotation == "size" {
		return false
	}
	return !w.now().Truncate(time.Hour).Equal(w.curSlot)
}

func (w *fileWriter) rotateLocked() error {
	if w.file == nil {
		return os.ErrClosed
	}
	// flush data out and drop the old file's page cache before switching
	if err := w.syncLocked(); err != nil {
		w.reportErr(err)
	}
	slot := w.rotationTime()
	next, err := w.openNext(slot)
	if err != nil {
		// keep writing to the old file instead of going dead; the caller
		// retries rotation on the next write once the path recovers
		return err
	}
	old := w.file
	w.file = next
	w.curSlot = slot
	w.curLen = 0
	w.dirty = 0
	w.lastSync = w.now()
	_ = old.Close()
	w.updateLink()
	w.cleanupLocked()
	return nil
}

// reportErr surfaces internal failures (a failed rotation, a failed fsync,
// a short write) on stderr. zap itself also reports write errors to its
// ErrorOutput on every entry, so this covers the paths where zap would not
// see anything, such as a background flush failure. Reports are rate
// limited: a persistent problem must stay visible without flooding stderr.
func (w *fileWriter) reportErr(err error) {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	now := w.now()
	if !w.lastErrReport.IsZero() && now.Sub(w.lastErrReport) < errReportInterval {
		return
	}
	w.lastErrReport = now
	fmt.Fprintf(os.Stderr, "[zlog] %s: %v (reports rate limited to one per %s)\n",
		w.cfg.name, err, errReportInterval)
}

func (w *fileWriter) syncLocked() error {
	if w.file == nil || w.dirty == 0 {
		return nil
	}
	err := syncAndDrop(w.file)
	w.dirty = 0
	w.lastSync = w.now()
	return err
}

func (w *fileWriter) open() error {
	if err := os.MkdirAll(w.cfg.dir, dirMode); err != nil {
		return err
	}
	slot := w.rotationTime()
	f, err := w.openNext(slot)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	w.file = f
	w.curSlot = slot
	w.curLen = st.Size()
	w.dirty = 0
	w.lastSync = w.now()
	// evict any stale page cache left by a previous process
	if st.Size() > 0 {
		_ = syncAndDrop(f)
	}
	w.updateLink()
	return nil
}

// rotationTime returns the timestamp that names the file which should be
// open right now: the current hour for time based rotation, the current
// instant for size based rotation.
func (w *fileWriter) rotationTime() time.Time {
	if w.cfg.rotation == "size" {
		return w.now()
	}
	return w.now().Truncate(time.Hour)
}

// openNext opens the file for the given rotation slot. It does not touch
// writer state, so a failure leaves the writer fully usable.
func (w *fileWriter) openNext(t time.Time) (*os.File, error) {
	return os.OpenFile(filepath.Join(w.cfg.dir, w.fileName(t)),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, fileMode)
}

// fileName returns the current file name: hourly when rotation is
// time-based (backward compatible), second-granularity when size-based.
func (w *fileWriter) fileName(t time.Time) string {
	if w.cfg.rotation == "size" {
		return w.uniqueName(fmt.Sprintf("%s-%s.log", w.cfg.name, t.Format(timeSecondLayout)))
	}
	return fmt.Sprintf("%s-%s.log", w.cfg.name, t.Format(timeHourLayout))
}

func (w *fileWriter) uniqueName(base string) string {
	if _, err := os.Stat(filepath.Join(w.cfg.dir, base)); os.IsNotExist(err) {
		return base
	}
	trimmed := strings.TrimSuffix(base, ".log")
	for i := 1; ; i++ {
		cand := fmt.Sprintf("%s-%d.log", trimmed, i)
		if _, err := os.Stat(filepath.Join(w.cfg.dir, cand)); os.IsNotExist(err) {
			return cand
		}
	}
}

// updateLink repoints <name>.log to the current file so tail/readers
// always follow the live log. The target is made absolute because a
// relative Path (go-zero's default is "logs") would otherwise create a
// link that resolves next to the link itself and dangles.
func (w *fileWriter) updateLink() {
	link := filepath.Join(w.cfg.dir, w.cfg.name+".log")
	target := w.file.Name()
	if abs, err := filepath.Abs(target); err == nil {
		target = abs
	}
	_ = os.Remove(link)
	_ = os.Symlink(target, link)
}

// parseFileName returns the timestamp embedded in a rotated file name. Only
// names that belong to this logger are accepted ("<name>-<timestamp>.log"),
// which keeps the retention pass from touching a sibling logger whose name
// merely starts with this one ("app" and "app-errors" sharing a directory).
func (w *fileWriter) parseFileName(name string) (time.Time, bool) {
	prefix := w.cfg.name + "-"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".log") {
		return time.Time{}, false
	}
	stem := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".log")
	// both layouts are accepted so files survive a change of Rotation mode
	for _, layout := range []string{timeHourLayout, timeSecondLayout} {
		if t, err := time.ParseInLocation(layout, stem, time.Local); err == nil {
			return t, true
		}
	}
	// size rotation appends "-1", "-2", ... to disambiguate a slot
	if i := strings.LastIndexByte(stem, '-'); i > 0 {
		if _, err := strconv.Atoi(stem[i+1:]); err == nil {
			if t, err := time.ParseInLocation(timeSecondLayout, stem[:i], time.Local); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

// cleanupLocked applies the retention rules to the directory:
//
//   - KeepDays removes files whose mtime is older than N days (go-zero
//     semantics).
//   - MaxBackups, exposed here as cfg.maxFiles, caps how many log files the
//     directory holds. The file currently being written counts towards the
//     limit and is never removed; the oldest rotated files go first.
//
// The two rules are independent and whichever is hit first wins, matching
// go-zero. Both work for time based and size based rotation.
func (w *fileWriter) cleanupLocked() {
	entries, err := os.ReadDir(w.cfg.dir)
	if err != nil {
		return
	}
	current := ""
	if w.file != nil {
		current = filepath.Base(w.file.Name())
	}
	now := w.now()

	type rotated struct {
		name string
		at   time.Time // timestamp embedded in the file name
		mod  time.Time // mtime, used by the KeepDays rule like go-zero
	}
	var files []rotated
	for _, e := range entries {
		if e.IsDir() || e.Name() == current {
			continue
		}
		at, ok := w.parseFileName(e.Name())
		if !ok {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		files = append(files, rotated{name: e.Name(), at: at, mod: info.ModTime()})
	}
	// newest first: the timestamp in the name is authoritative, so a file
	// whose mtime was touched by an external tool still ages correctly
	sort.Slice(files, func(i, j int) bool {
		if files[i].at.Equal(files[j].at) {
			return files[i].name > files[j].name
		}
		return files[i].at.After(files[j].at)
	})

	removed := 0
	for i, f := range files {
		expired := w.cfg.keepDays > 0 &&
			now.Sub(f.mod) > time.Duration(w.cfg.keepDays)*24*time.Hour
		// the open file is already excluded above, so only maxFiles-1
		// rotated files may stay for the directory to hold maxFiles files
		tooMany := w.cfg.maxFiles > 0 && i >= w.cfg.maxFiles-1
		if !expired && !tooMany {
			continue
		}
		if err := os.Remove(filepath.Join(w.cfg.dir, f.name)); err == nil {
			removed++
		} else {
			w.reportErr(err)
		}
	}
	if removed > 0 {
		w.reportRetention(removed, len(files)-removed+1)
	}
}

// reportRetention records what a retention pass removed so an operator can
// tell a too small MaxBackups from a too quiet service.
func (w *fileWriter) reportRetention(removed, kept int) {
	fmt.Fprintf(os.Stderr, "[zlog] %s: retained %d log files, removed %d\n",
		w.cfg.name, kept, removed)
}
