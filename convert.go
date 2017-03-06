// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"reflect"
)

// Convert T to []interface{} or map[string]interface{}; []T or map[]T to
// [][]interface{}, []map[string]interface{}, map[PK][]interface{} or map[PK]map[string]interface{}.
func Convert(collapse, encoding bool, dst, src interface{}, columns ...string) error {
	t, a := newTable(src)
	cols := t.Filter(columns...)
	if cols.Empty() {
		return t.errNoColumns()
	}
	v, _ := ptrElem(src)
	if dst == nil {
		panic("huge: nil")
	}
	if a[0] != nil || a[1] != nil {
		return convertAll(collapse, encoding, dst, v, t, cols)
	} else if a[0] == nil && a[1] == nil {
		return convertOne(collapse, encoding, dst, v, cols)
	}
	panic("huge: type unsupported")
}

func convertOne(collapse, encoding bool, dst interface{}, v reflect.Value, cols Columns) error {
	var a []interface{}
	var m map[string]interface{}
	switch i := dst.(type) {
	case *map[string]interface{}:
		if i == nil {
			panic("huge: nil pointer")
		} else if *i == nil {
			m = make(map[string]interface{}, len(cols))
			*i = m
		} else {
			m = *i
		}
	case map[string]interface{}:
		if i == nil {
			panic("huge: nil map")
		} else {
			m = i
		}
	case *[]interface{}:
		if i == nil {
			panic("huge: nil pointer")
		} else if cap(*i) < len(cols) {
			a = make([]interface{}, len(cols))
		} else if len(*i) < len(cols) {
			a = (*i)[:len(cols)]
		}
		*i = a
	case []interface{}:
		if len(i) < len(cols) {
			panic("huge: length")
		} else {
			a = i
		}
	default:
		panic("huge: type unsupported")
	}
	for i, c := range cols {
		if j, err := c.getBy(collapse, encoding, v); err != nil {
			return err
		} else if a != nil {
			a[i] = j
		} else {
			m[c.Name] = j
		}
	}
	return nil
}

func convertAll(collapse, encoding bool, dst interface{}, v reflect.Value, t *Table, cols Columns) error {
	x, p := ptrElem(dst)
	or := func(t reflect.Type) (i int) {
		switch t.Kind() {
		case reflect.Map:
			if t.Key() == typeString && t.Elem() == typeInterface {
				i = 2
			}
		case reflect.Slice:
			if t.Elem() == typeInterface {
				i = 1
			}
		}
		return
	}
	c, o := t.PrimaryKey(), 0
	switch y := x.Type(); y.Kind() {
	case reflect.Map:
		if c == nil {
			panic(t.errNoPrimaryKey())
		} else if c.last().t != y.Key() {
			panic("huge: type unsupported")
		} else if o = or(y.Elem()); o > 0 {
			if x.IsNil() {
				if p {
					x.Set(reflect.MakeMap(y))
				} else {
					panic("huge: nil map")
				}
			}
		}
	case reflect.Slice:
		if o = or(y.Elem()); o > 0 {
			if n := v.Len(); p {
				if x.Cap() < n {
					x.Set(reflect.MakeSlice(y, n, n))
				} else if x.Len() < n {
					x.SetLen(n)
				}
			} else if x.Len() < n {
				panic("huge: length")
			}
		}
	}
	if o == 0 {
		panic("huge: type unsupported")
	}
	one := func(i int, v reflect.Value) error {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return t.errNil()
			} else {
				v = v.Elem()
			}
		}
		var y reflect.Value
		var a []interface{}
		var m map[string]interface{}
		switch x.Kind() {
		case reflect.Map:
			var ok bool
			if y, ok = c.field(v); !ok {
				return c.errGet()
			}
		case reflect.Slice:
			y = x.Index(i)
		default:
			panic(false)
		}
		switch o {
		case 1:
			a = make([]interface{}, len(cols))
		case 2:
			m = make(map[string]interface{}, len(cols))
		default:
			panic(false)
		}
		for i, c := range cols {
			if j, err := c.getBy(collapse, encoding, v); err != nil {
				return err
			} else if a != nil {
				a[i] = j
			} else {
				m[c.Name] = j
			}
		}
		if a != nil {
			v = reflect.ValueOf(a)
		} else {
			v = reflect.ValueOf(m)
		}
		switch x.Kind() {
		case reflect.Map:
			if x.MapIndex(y).IsValid() {
				return c.errDuplicate()
			} else {
				x.SetMapIndex(y, v)
			}
		case reflect.Slice:
			y.Set(v)
		}
		return nil
	}
	switch v.Kind() {
	case reflect.Map:
		for i, k := range v.MapKeys() {
			if err := one(i, v.MapIndex(k)); err != nil {
				return err
			}
		}
	case reflect.Slice:
		for i, n := 0, v.Len(); i < n; i++ {
			if err := one(i, v.Index(i)); err != nil {
				return err
			}
		}
	default:
		panic(false)
	}
	return nil
}
