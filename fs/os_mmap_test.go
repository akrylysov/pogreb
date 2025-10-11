package fs

import (
	"testing"
)

func TestOSMMapFS(t *testing.T) {
	testFS(t, Sub(OSMMap, t.TempDir()))
}
