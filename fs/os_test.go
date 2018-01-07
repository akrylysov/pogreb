package fs

import (
	"testing"
)

func TestOSLockFile(t *testing.T) {
	testLockFile(OS, t)
}

func TestOSLockFileNeedsRecovery(t *testing.T) {
	testLockFileNeedsRecovery(OS, t)
}
