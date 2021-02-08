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

// https://github.com/golang/go/blob/go1.15/src/reflect/value.go#L1071
var nillableKinds = map[reflect.Kind]bool{
	reflect.Chan:          true,
	reflect.Func:          true,
	reflect.Map:           true,
	reflect.Ptr:           true,
	reflect.UnsafePointer: true,
	reflect.Interface:     true,
	reflect.Slice:         true,
}

// Nil fails the test when obj is not nil.
func Nil(t testing.TB, obj interface{}) {
	if obj == nil {
		return
	}
	val := reflect.ValueOf(obj)
	if !nillableKinds[val.Kind()] || !val.IsNil() {
		t.Helper()
		t.Fatalf("expected nil; got %+v", obj)
	}
}

// NotNil fails the test when obj is nil.
func NotNil(t testing.TB, obj interface{}) {
	val := reflect.ValueOf(obj)
	if obj == nil || (nillableKinds[val.Kind()] && val.IsNil()) {
		t.Helper()
		t.Fatalf("expected not nil; got %+v", obj)
	}
}

const pollingInterval = time.Millisecond * 10 // How often CompleteWithin polls the cond function.

// CompleteWithin fails the test when cond doesn't succeed within waitDur.
func CompleteWithin(t testing.TB, waitDur time.Duration, cond func() bool) {
	start := time.Now()
	for time.Since(start) < waitDur {
		if cond() {
			return
		}
		time.Sleep(pollingInterval)
	}
	t.Helper()
	t.Fatalf("expected to complete within %v", waitDur)
}

// Panic fails the test when the test doesn't panic with the expected message.
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
