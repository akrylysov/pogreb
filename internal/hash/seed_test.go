package hash

import (
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestRandSeed(t *testing.T) {
	_, err := RandSeed()
	assert.Nil(t, err)
}
