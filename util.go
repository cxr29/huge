// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"reflect"
	"time"
)

func isInts(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	}
	return false
}

func isUints(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

func isIntegers(t reflect.Type) bool {
	k := t.Kind()
	return isInts(k) || isUints(k)
}

func isSeconds(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int32, reflect.Uint, reflect.Uint32:
		return true
	}
	return false
}

func isMilliseconds(k reflect.Kind) bool {
	return k == reflect.Int64 || k == reflect.Uint64
}

func isTimes(t reflect.Type) bool {
	k := t.Kind()
	return isSeconds(k) || isMilliseconds(k) || t == typeTime
}

func isStruct(t reflect.Type) bool {
	return t.Kind() == reflect.Struct
}

func isStructs(t reflect.Type) bool {
	if isMapOrSlice(t.Kind()) {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		return isStruct(t)
	}
	return false
}

func isMapOrSlice(k reflect.Kind) bool {
	return k == reflect.Map || k == reflect.Slice
}

func canNil(k reflect.Kind) bool {
	switch k {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return true
	}
	return false
}

func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return v.IsValid() && v.Type() == typeTime && v.Interface().(time.Time).IsZero()
}
