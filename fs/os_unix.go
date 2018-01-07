// +build !windows

package fs

import (
	"os"
	"syscall"
	"unsafe"
)

func mmap(f *os.File, size int64) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmap(data []byte) error {
	return syscall.Munmap(data)
}

func madviceRandom(data []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(syscall.MADV_RANDOM))
	if errno != 0 {
		return errno
	}
	return nil
}
