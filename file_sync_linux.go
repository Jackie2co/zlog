//go:build linux && (amd64 || arm64)

package zlog

import (
	"os"
	"syscall"
)

const fadvDontNeed = 4 // POSIX_FADV_DONTNEED

// syncAndDrop flushes the file's data to disk and evicts the file's pages
// from the kernel page cache. Log files are append-only and never re-read,
// so after writeback the cached pages are pure waste: without this hint a
// busy log keeps growing the cgroup's page cache (visible as inflated
// Memory in systemd/systemd-oomd). The fadvise call is only a hint, its
// errors are ignored, but a failed fdatasync means the data is not durable
// yet and is reported to the caller.
func syncAndDrop(f *os.File) error {
	fd := f.Fd()
	_, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, fd, 0, 0)
	_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64, fd, 0, 0, fadvDontNeed, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
