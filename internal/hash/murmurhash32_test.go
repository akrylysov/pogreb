package hash

import (
	"testing"
)

func TestSum32WithSeed(t *testing.T) {
	// TODO: test all code paths
	actual := Sum32WithSeed([]byte{1, 2, 3, 4, 5}, 0)
	expected := uint32(2727459272)
	if actual != expected {
		t.Fatalf("expected: %d, actual: %d", expected, actual)
	}
}

func BenchmarkSum32WithSeed(b *testing.B) {
	data := []byte("pogreb_Sum32WithSeed_bench")
	b.SetBytes(int64(len(data)))
	for n := 0; n < b.N; n++ {
		Sum32WithSeed(data, 0)
	}
}
