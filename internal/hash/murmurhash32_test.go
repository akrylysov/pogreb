package hash

import (
	"fmt"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestSum32WithSeed(t *testing.T) {
	testCases := []struct {
		in   []byte
		seed uint32
		out  uint32
	}{
		{
			in:  nil,
			out: 0,
		},
		{
			in:   nil,
			seed: 1,
			out:  1364076727,
		},
		{
			in:  []byte{1},
			out: 3831157163,
		},
		{
			in:  []byte{1, 2},
			out: 1690789502,
		},
		{
			in:  []byte{1, 2, 3},
			out: 2161234436,
		},
		{
			in:  []byte{1, 2, 3, 4},
			out: 1043635621,
		},
		{
			in:  []byte{1, 2, 3, 4, 5},
			out: 2727459272,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert.Equal(t, tc.out, Sum32WithSeed(tc.in, tc.seed))
		})
	}
}

func BenchmarkSum32WithSeed(b *testing.B) {
	data := []byte("pogreb_Sum32WithSeed_bench")
	b.SetBytes(int64(len(data)))
	for n := 0; n < b.N; n++ {
		Sum32WithSeed(data, 0)
	}
}
