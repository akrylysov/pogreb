//go:build plan9
// +build plan9

package fs

import (
	"os"
	"syscall"
)

func createLockFile(name string, perm os.FileMode) (LockFile, bool, error) {
	acquiredExisting := false
	if _, err := os.Stat(name); err == nil {
		acquiredExisting = true
	}
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, syscall.DMEXCL|perm)
	if err != nil {
		return nil, false, err
	}
	return &osLockFile{f, name}, acquiredExisting, nil
}

// Return a default FileSystem for this platform.
func DefaultFileSystem() FileSystem {
	return OS
}
