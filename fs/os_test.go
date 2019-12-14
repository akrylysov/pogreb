package fs

import (
	"testing"
)

func TestOSLockFile(t *testing.T) {
	testLockFile(OS, t)
}

func TestOSLockAcquireExisting(t *testing.T) {
	testLockFileAcquireExisting(OS, t)
}
