package pogreb

import (
	"testing"

	"github.com/akrylysov/pogreb/fs"
)

func TestFreelistAllocate(t *testing.T) {
	testCases := []struct {
		frees                          []block
		expectedBlocks                 []block
		allocations                    []uint32
		expectedOffsets                []int64
		expectedBlocksAfterAllocations []block
	}{
		// Empty
		{
			[]block{},
			nil,
			[]uint32{1},
			[]int64{-1},
			nil,
		},
		// 1
		{
			[]block{{2, 2}},
			[]block{{2, 2}},
			[]uint32{10, 1, 1, 1},
			[]int64{-1, 2, 3, -1},
			[]block{},
		},
		// 2
		{
			[]block{{2, 2}, {1, 1}},
			[]block{{1, 1}, {2, 2}},
			[]uint32{10, 1, 2},
			[]int64{-1, 1, 2},
			[]block{},
		},
		// 3
		{
			[]block{{2, 2}, {1, 1}, {3, 3}},
			[]block{{1, 1}, {2, 2}, {3, 3}},
			[]uint32{4, 10, 1, 2, 3},
			[]int64{-1, -1, 1, 2, 3},
			[]block{},
		},
		// 4
		{
			[]block{{2, 2}, {1, 1}, {3, 3}, {10, 10}},
			[]block{{1, 1}, {2, 2}, {3, 3}, {10, 10}},
			[]uint32{11, 1, 2, 3, 4},
			[]int64{-1, 1, 2, 3, 10},
			[]block{{14, 6}},
		},
	}
	for _, testCase := range testCases {
		l := freelist{}
		for _, block := range testCase.frees {
			l.free(block.offset, block.size)
		}
		assertDeepEqual(t, testCase.expectedBlocks, l.blocks)
		for i := 0; i < len(testCase.allocations); i++ {
			if off := l.allocate(testCase.allocations[i]); off != testCase.expectedOffsets[i] {
				t.Fatal(i)
			}
		}
		assertDeepEqual(t, testCase.expectedBlocksAfterAllocations, l.blocks)
	}
}

func TestFreelistDefrag(t *testing.T) {
	testCases := []struct {
		in  []block
		out []block
	}{
		// Empty
		{
			[]block{},
			[]block{},
		},
		// 1
		{
			[]block{{2, 2}},
			[]block{{2, 2}},
		},
		// 2
		{
			[]block{{4, 2}, {0, 4}},
			[]block{{0, 6}},
		},
		// 3
		{
			[]block{{4, 2}, {0, 4}, {6, 6}},
			[]block{{0, 12}},
		},
		// None
		{
			[]block{{4, 2}, {0, 3}, {7, 6}},
			[]block{{4, 2}, {0, 3}, {7, 6}},
		},
		// Front
		{
			[]block{{4, 2}, {0, 4}, {8, 8}},
			[]block{{0, 6}, {8, 8}},
		},
		// Middle
		{
			[]block{{0, 1}, {3, 2}, {5, 3}, {9, 9}},
			[]block{{0, 1}, {3, 5}, {9, 9}},
		},
		// Back
		{
			[]block{{4, 2}, {0, 3}, {6, 6}},
			[]block{{0, 3}, {4, 8}},
		},
	}
	for _, testCase := range testCases {
		l := freelist{testCase.in}
		l.defrag()
		assertDeepEqual(t, testCase.out, l.blocks)
	}
}

func TestFreelistSerialization(t *testing.T) {
	l := freelist{[]block{{1, 1}, {2, 2}, {3, 3}, {10, 10}}}
	fs.Mem.Remove("test")
	f, _ := openFile(fs.Mem, "test", 0, 0)
	off, err := l.write(f)
	if err != nil {
		t.Fatal(err)
	}
	fstat, _ := f.Stat()
	l2 := freelist{}
	err = l2.read(f, off)
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, []block{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {off, uint32(fstat.Size())}}, l2.blocks)
}
