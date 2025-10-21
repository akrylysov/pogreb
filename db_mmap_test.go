//go:build !plan9
// +build !plan9

package pogreb

import (
	"github.com/akrylysov/pogreb/fs"
)

var testFileSystems = []fs.FileSystem{fs.Mem, fs.OSMMap, fs.OS}
