package assert

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEqual(t *testing.T) {
	testCases := []struct {
		first          interface{}
		second         interface{}
		expectedFailed bool
	}{
		{
			first:          1,
			second:         1,
			expectedFailed: false,
		},

		{
			first:          nil,
			second:         nil,
			expectedFailed: false,
		},
		{
			first:          "1",
			second:         "1",
			expectedFailed: false,
		},
		{
			first:          struct{}{},
			second:         struct{}{},
			expectedFailed: false,
		},
		{
			first:          struct{ x int }{x: 1},
			second:         struct{ x int }{x: 1},
			expectedFailed: false,
		},
		{
			first:          1,
			second:         2,
			expectedFailed: true,
		},
		{
			first:          1,
			second:         "1",
			expectedFailed: true,
		},
		{
			first:          1,
			second:         1.0,
			expectedFailed: true,
		},
		{
			first:          struct{ x int }{x: 1},
			second:         struct{ x int }{x: 2},
			expectedFailed: true,
		},
		{
			first:          struct{ x int }{x: 1},
			second:         struct{ y int }{y: 1},
			expectedFailed: true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d %+v", i, tc), func(t *testing.T) {
			mock := &testing.T{}
			wg := &sync.WaitGroup{}
			wg.Add(1)
			// Run the asserting in a goroutine. t.Fatal calls runtime.Goexit.
			go func() {
				defer wg.Done()
				Equal(mock, tc.first, tc.second)
			}()
			wg.Wait()
			failed := mock.Failed()
			if tc.expectedFailed != failed {
				t.Fatalf("expected to fail: %t; failed: %t", tc.expectedFailed, failed)
			}
		})
	}
}

func TestNil(t *testing.T) {
	var nilIntPtr *int
	var nilStructPtr *struct{ x int }
	var nilSlice []string

	testCases := []struct {
		obj   interface{}
		isNil bool
	}{
		{
			obj:   nil,
			isNil: true,
		},
		{
			obj:   nilIntPtr,
			isNil: true,
		},
		{
			obj:   nilStructPtr,
			isNil: true,
		},
		{
			obj:   nilSlice,
			isNil: true,
		},
		{
			obj:   1,
			isNil: false,
		},
		{
			obj:   "1",
			isNil: false,
		},
		{
			obj:   []string{},
			isNil: false,
		},
		{
			obj:   [2]int{1, 1},
			isNil: false,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d %+v", i, tc.obj), func(t *testing.T) {
			mockNil := &testing.T{}
			mockNotNil := &testing.T{}
			wg := &sync.WaitGroup{}
			wg.Add(2)
			go func() {
				defer wg.Done()
				Nil(mockNil, tc.obj)
			}()
			go func() {
				defer wg.Done()
				NotNil(mockNotNil, tc.obj)
			}()
			wg.Wait()
			if tc.isNil == mockNil.Failed() {
				t.Fatalf("Nil expected to fail: %t; failed: %t", !tc.isNil, mockNil.Failed())
			}
			if !tc.isNil == mockNotNil.Failed() {
				t.Fatalf("NotNil expected to fail: %t; failed: %t", tc.isNil, mockNotNil.Failed())
			}
		})
	}
}

func TestPanic(t *testing.T) {
	testCases := []struct {
		name           string
		f              func()
		expectedFailed bool
	}{
		{
			name: "panic",
			f: func() {
				panic("message123")
			},
			expectedFailed: false,
		},
		{
			name: "panic: wrong message",
			f: func() {
				panic("message456")
			},
			expectedFailed: true,
		},
		{
			name:           "no panic",
			f:              func() {},
			expectedFailed: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &testing.T{}
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				Panic(mock, "message123", tc.f)
			}()
			wg.Wait()
			if tc.expectedFailed != mock.Failed() {
				t.Fatalf("expected to fail: %t; failed: %t", tc.expectedFailed, mock.Failed())
			}
		})
	}
}

func TestCompleteWithin(t *testing.T) {
	var tc2Tries int
	var tc4Tries int
	testCases := []struct {
		name           string
		dur            time.Duration
		cond           func() bool
		expectedFailed bool
	}{
		{
			name: "completed: first try",
			dur:  time.Hour,
			cond: func() bool {
				return true
			},
			expectedFailed: false,
		},
		{
			name: "completed: second try",
			dur:  time.Hour,
			cond: func() bool {
				if tc2Tries == 0 {
					tc2Tries++
					return false
				}
				return true
			},
			expectedFailed: false,
		},
		{
			name: "not completed",
			dur:  time.Nanosecond,
			cond: func() bool {
				return false
			},
			expectedFailed: true,
		},
		{
			name: "not completed: timeout",
			dur:  time.Nanosecond,
			cond: func() bool {
				if tc4Tries == 0 {
					tc4Tries++
					time.Sleep(pollingInterval * 2)
					return false
				}
				return true
			},
			expectedFailed: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &testing.T{}
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				CompleteWithin(mock, tc.dur, tc.cond)
			}()
			wg.Wait()
			if tc.expectedFailed != mock.Failed() {
				t.Fatalf("expected to fail: %t; failed: %t", tc.expectedFailed, mock.Failed())
			}
		})
	}
}
