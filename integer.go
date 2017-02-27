// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"reflect"
)

func (c *Column) convertInteger(i int64) interface{} {
	f := c.last()
	t := f.Type()
	if k := t.Kind(); isInts(k) {
		p := reflect.New(t)
		q := p.Elem()
		if !q.OverflowInt(i) {
			q.SetInt(i)
			if f.Is(oPointer) {
				return p.Interface()
			}
			return q.Interface()
		}
	} else if isUints(k) {
		p := reflect.New(t)
		q := p.Elem()
		if u := uint64(i); !q.OverflowUint(u) {
			q.SetUint(u)
			if f.Is(oPointer) {
				return p.Interface()
			}
			return q.Interface()
		}
	}
	return nil
}

func (c *Column) getInteger(v reflect.Value) (int64, bool) {
	if v, ok := c.field(v); ok {
		if f := c.last(); f.Is(oPointer) {
			if v.IsNil() {
				return 0, false
			}
			v = v.Elem()
		}
		if k := v.Kind(); isInts(k) {
			return v.Int(), true
		} else if isUints(k) {
			return int64(v.Uint()), true
		}
	}
	return 0, false
}

func (c *Column) setInteger(v reflect.Value, i int64) bool {
	if v, ok := c.field(v); ok {
		if f := c.last(); f.Is(oPointer) {
			if v.IsNil() {
				if v.CanSet() {
					v.Set(reflect.New(f.Type()))
				} else {
					return false
				}
			}
			v = v.Elem()
		}
		if v.CanSet() {
			if k := v.Kind(); isInts(k) {
				if !v.OverflowInt(i) {
					v.SetInt(i)
					return true
				}
			} else if isUints(k) {
				if u := uint64(i); !v.OverflowUint(u) {
					v.SetUint(u)
					return true
				}
			}
		}
	}
	return false
}
