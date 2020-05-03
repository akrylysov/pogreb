package assert

import (
	"reflect"
	"testing"
	"time"
)

// Equal fails the test when expected is not equal to actual.
func Equal(t testing.TB, expected interface{}, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		t.Helper()
		t.Fatalf("expected %+v; got %+v", expected, actual)
	}
}

// Nil fails the test when actual is not nil.
func Nil(t testing.TB, actual interface{}) {
	if actual != nil && !reflect.ValueOf(actual).IsNil() {
		t.Helper()
		t.Fatalf("expected nil; got %+v", actual)
	}
}

// CompleteWithin fails the test when cond doesn't succeed within waitDur.
func CompleteWithin(t testing.TB, waitDur time.Duration, cond func() bool) {
	start := time.Now()
	for time.Since(start) < waitDur {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	t.Helper()
	t.Fatalf("expected to complete within %v", waitDur)
}

// Panic fails the test when the test doesn't panic.
func Panic(t testing.TB, expectedMessage string, f func()) {
	t.Helper()
	var message interface{}
	func() {
		defer func() {
			message = recover()
		}()
		f()
	}()
	Equal(t, expectedMessage, message)
}
