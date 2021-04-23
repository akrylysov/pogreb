package errors

import (
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestWrap(t *testing.T) {
	err1 := New("err1")
	w11 := Wrap(err1, "wrapped 11")
	w12 := Wrapf(w11, "wrapped %d%s", 1, "2")

	assert.Equal(t, err1, w11.(wrappedError).Unwrap())
	assert.Equal(t, w11, w12.(wrappedError).Unwrap())

	assert.Equal(t, "wrapped 11: err1", w11.Error())
	assert.Equal(t, "wrapped 12: wrapped 11: err1", w12.Error())
}
