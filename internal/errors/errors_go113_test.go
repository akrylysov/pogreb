// +build go1.13

package errors

import (
	"errors"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestIs(t *testing.T) {
	err1 := New("err1")
	w11 := Wrap(err1, "wrapped 11")
	w12 := Wrap(w11, "wrapped 12")

	err2 := New("err2")
	w21 := Wrap(err2, "wrapped 21")

	assert.Equal(t, true, errors.Is(err1, err1))
	assert.Equal(t, true, errors.Is(w11, err1))
	assert.Equal(t, true, errors.Is(w12, err1))
	assert.Equal(t, true, errors.Is(w12, w11))

	assert.Equal(t, false, errors.Is(err1, err2))
	assert.Equal(t, false, errors.Is(w11, err2))
	assert.Equal(t, false, errors.Is(w12, err2))
	assert.Equal(t, false, errors.Is(w21, err1))
	assert.Equal(t, false, errors.Is(w21, w11))
}
