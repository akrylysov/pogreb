package fs

import (
	"testing"
)

func TestOSFS(t *testing.T) {
	testFS(t, Sub(OS, t.TempDir()))
}

func TestOSLockFile(t *testing.T) {
	testLockFile(t, Sub(OS, t.TempDir()))
}

func TestOSLockAcquireExisting(t *testing.T) {
	testLockFileAcquireExisting(t, Sub(OS, t.TempDir()))
}
