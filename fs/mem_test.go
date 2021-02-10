package fs

import (
	"testing"
)

func TestMemFS(t *testing.T) {
	testFS(t, Mem)
}

func TestMemLockFile(t *testing.T) {
	testLockFile(t, Mem)
}

func TestMemLockAcquireExisting(t *testing.T) {
	testLockFileAcquireExisting(t, Mem)
}
