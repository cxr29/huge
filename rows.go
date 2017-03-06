// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"database/sql"
	"errors"
	"reflect"

	"github.com/cxr29/log"
)

type Rows struct {
	err      error
	rows     *sql.Rows
	DealName func(string) string
}

func (r *Rows) Close() error {
	if r.err != nil {
		return r.err
	}
	return r.rows.Close()
}
func (r *Rows) Columns() ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.rows.Columns()
}
func (r *Rows) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.rows.Err()
}
func (r *Rows) Next() bool {
	if r.err != nil {
		return false
	}
	return r.rows.Next()
}

func (r *Rows) scanArray(v reflect.Value) error {
	n := v.Len()
	a := make([]interface{}, n)
	for i := 0; i < n; i++ {
		a[i] = v.Index(i).Interface()
	}
	return r.rows.Scan(a...)
}

func (r *Rows) scanMap(columns []string, v reflect.Value) error {
	m := make(map[string]int, len(columns))
	for i, s := range columns {
		if r.DealName != nil {
			s = r.DealName(s)
		}
		if _, ok := m[s]; ok {
			return errors.New("huge: duplicate column: " + s)
		} else {
			m[s] = i
		}
	}
	a := make([]interface{}, len(columns))
	f := make([]scanNewFunc, len(columns))
	if t := v.Type().Elem(); t == typeInterface && v.Len() > 0 {
		for _, k := range v.MapKeys() {
			s := k.String()
			if i, ok := m[s]; !ok {
				return errors.New("huge: column not exist: " + s)
			} else {
				a[i], f[i] = scanNew(v.MapIndex(k).Interface())
			}
		}
		for i := range a {
			if a[i] == nil {
				var j interface{}
				a[i] = &j
			}
		}
	} else {
		for i := range a {
			a[i] = reflect.New(t).Interface()
		}
	}
	if err := r.rows.Scan(a...); err != nil {
		return err
	}
	for s, i := range m {
		if j := f[i]; j != nil {
			if k, err := j(); err != nil {
				return err
			} else {
				v.SetMapIndex(reflect.ValueOf(s), k)
			}
		} else {
			v.SetMapIndex(reflect.ValueOf(s), reflect.ValueOf(a[i]).Elem())
		}
	}
	return nil
}

func (r *Rows) scanSlice(n int, v reflect.Value) error {
	a := make([]interface{}, n)
	f := make([]scanNewFunc, n)
	if t := v.Type().Elem(); t == typeInterface {
		for i := 0; i < n; i++ {
			a[i], f[i] = scanNew(v.Index(i).Interface())
		}
	} else {
		for i := 0; i < n; i++ {
			a[i] = reflect.New(t).Interface()
		}
	}
	if err := r.rows.Scan(a...); err != nil {
		return err
	}
	for i, j := range f {
		if j != nil {
			if k, err := j(); err != nil {
				return err
			} else {
				v.Index(i).Set(k)
			}
		} else {
			v.Index(i).Set(reflect.ValueOf(a[i]).Elem())
		}
	}
	return nil
}

func (r *Rows) scanStruct(t *Table, columns []string, v reflect.Value) error {
	m := make(map[int]struct{}, len(columns))
	a := make([]interface{}, len(columns))
	f := make([]func() error, len(columns))
	for i, s := range columns {
		if r.DealName != nil {
			s = r.DealName(s)
		}
		if c := t.Find(s); c == nil {
			return errors.New("huge: column not found: " + s)
		} else if _, ok := m[c.i]; ok {
			return errors.New("huge: duplicate column: " + s)
		} else if a[i], f[i], ok = c.scan(v); ok {
			m[c.i] = struct{}{}
		} else {
			return c.errSet()
		}
	}
	if err := r.rows.Scan(a...); err != nil {
		return err
	}
	for _, i := range f {
		if i != nil {
			if err := i(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Scan T, [] or map[string]. For straightforward Scan just given an array.
func (r *Rows) Scan(i interface{}) error {
	if r.err != nil {
		return r.err
	}
	v, p := ptrElem(i)
	columns, err := r.rows.Columns()
	if err != nil {
		return err
	}
	switch v.Kind() {
	case reflect.Array:
		if v.Len() == len(columns) {
			return r.scanArray(v)
		} else {
			panic("huge: length")
		}
	case reflect.Map:
		if t := v.Type(); t.Key() == typeString {
			if v.IsNil() {
				if p {
					v.Set(reflect.MakeMap(t))
				} else {
					panic("huge: nil map")
				}
			}
			return r.scanMap(columns, v)
		} else {
			panic("huge: not map[string]")
		}
	case reflect.Slice:
		if n := len(columns); p {
			if v.Cap() < n {
				v.Set(reflect.MakeSlice(v.Type(), n, n))
			} else if v.Len() < n {
				v.SetLen(n)
			}
			return r.scanSlice(len(columns), v)
		} else if v.Len() < n {
			panic("huge: length")
		}
	case reflect.Struct:
		return r.scanStruct(newTableBy(v.Type()), columns, v)
	}
	panic("huge: type unsupported")
}

// One Scan and Close, be careful with sql.RawBytes.
func (r *Rows) One(i interface{}) (ok bool, err error) {
	if r.err != nil {
		return false, r.err
	}
	defer func() {
		log.ErrWarning(r.rows.Close())
	}()
	if r.rows.Next() {
		err = r.Scan(i)
		ok = err == nil
		if ok {
			err = r.rows.Close()
		}
	} else {
		err = r.rows.Err()
	}
	return
}

func (r *Rows) allSlice(columns []string, a []interface{}, t reflect.Type, v reflect.Value) (err error) {
	if v.Len() > 0 {
		v.SetLen(0)
	}
	b := make([]interface{}, len(a))
	f := make([]scanNewFunc, len(a))
	x := v.Type().Elem()
	y := x.Kind() == reflect.Map
	var k reflect.Value
Loop:
	for r.rows.Next() {
		for i, j := range a {
			if j != nil {
				b[i], f[i] = scanNew(j)
			} else {
				b[i] = reflect.New(t)
				f[i] = nil
			}
		}
		if err = r.rows.Scan(b...); err != nil {
			break
		}
		if y {
			m := reflect.MakeMap(x)
			for i, j := range f {
				if j != nil {
					if k, err = j(); err != nil {
						break Loop
					}
				} else {
					k = reflect.ValueOf(b[i]).Elem()
				}
				m.SetMapIndex(reflect.ValueOf(columns[i]), k)
			}
			v.Set(reflect.Append(v, m))
		} else {
			s := reflect.MakeSlice(x, len(b), len(b))
			for i, j := range f {
				if j != nil {
					if k, err = j(); err != nil {
						break Loop
					}
				} else {
					k = reflect.ValueOf(b[i]).Elem()
				}
				s.Index(i).Set(k)
			}
			v.Set(reflect.Append(v, s))
		}
	}
	if err == nil {
		err = r.Err()
	}
	if err == nil {
		err = r.Close()
	}
	return err
}

func (r *Rows) allStruct(columns []string, t *Table, v reflect.Value) (err error) {
	a := make(Columns, len(columns))
	m := make(map[int]struct{}, len(columns))
	for i, s := range columns {
		if r.DealName != nil {
			s = r.DealName(s)
		}
		if c := t.Find(s); c == nil {
			return errors.New("huge: column not found: " + s)
		} else if _, ok := m[c.i]; ok {
			return errors.New("huge: duplicate column: " + s)
		} else {
			a[i] = c
			m[c.i] = struct{}{}
		}
	}
	c := t.PrimaryKey()
	x := v.Kind() == reflect.Map
	y := v.Type().Elem().Kind() == reflect.Ptr
	if x {
		if _, ok := m[c.i]; !ok {
			return errors.New("huge: primary key column not exist")
		}
	} else if v.Len() > 0 {
		v.SetLen(0)
	}
	b := make([]interface{}, len(a))
	f := make([]func() error, len(a))
	for r.rows.Next() {
		p := reflect.New(t.s.t)
		q := p.Elem()
		for i, c := range a {
			var ok bool
			b[i], f[i], ok = c.scan(q)
			if !ok {
				return c.errSet()
			}
		}
		if err = r.rows.Scan(b...); err != nil {
			break
		}
		for _, i := range f {
			if i != nil {
				if err = i(); err != nil {
					break
				}
			}
		}
		if x {
			if k, ok := c.field(q); ok {
				if v.MapIndex(k).IsValid() {
					err = c.errDuplicate()
					break
				} else if y {
					v.SetMapIndex(k, p)
				} else {
					v.SetMapIndex(k, q)
				}
			} else {
				err = c.errGet()
				break
			}
		} else if y {
			v.Set(reflect.Append(v, p))
		} else {
			v.Set(reflect.Append(v, q))
		}
	}
	if err == nil {
		err = r.Err()
	}
	if err == nil {
		err = r.Close()
	}
	return err
}

// All *[]T, map[PK]T, *[][] or *[]map[string].
func (r *Rows) All(i interface{}) error {
	if r.err != nil {
		return r.err
	}
	defer func() {
		log.ErrWarning(r.rows.Close())
	}()
	v, p := ptrElem(i)
	columns, err := r.Columns()
	if err != nil {
		return err
	}
	switch v.Kind() {
	case reflect.Map:
		t, types := newTable(i)
		if c := t.PrimaryKey(); c == nil {
			panic(t.errNoPrimaryKey())
		} else if c.last().t != types[0].Key() {
			panic("huge: type unsupported")
		}
		if v.IsNil() {
			if p {
				v.Set(reflect.MakeMap(types[0]))
			} else {
				panic("huge: nil map")
			}
		}
		return r.allStruct(columns, t, v)
	case reflect.Slice:
		if !p {
			panic("huge: not pointer")
		}
		switch t := v.Type().Elem(); t.Kind() {
		case reflect.Map:
			if t.Key() != typeString {
				panic("huge: type unsupported")
			}
			a := make([]interface{}, len(columns))
			m := make(map[string]int, len(columns))
			for i, s := range columns {
				if r.DealName != nil {
					s = r.DealName(s)
				}
				if _, ok := m[s]; ok {
					return errors.New("huge: duplicate column: " + s)
				} else {
					m[s] = i
					columns[i] = s
				}
			}
			if t = t.Elem(); t == typeInterface && v.Len() > 0 {
				x := v.Index(0)
				for _, k := range x.MapKeys() {
					s := k.String()
					if i, ok := m[s]; !ok {
						return errors.New("huge: column not exist: " + s)
					} else {
						a[i] = x.MapIndex(k).Interface()
					}
				}
			}
			return r.allSlice(columns, a, t, v)
		case reflect.Slice:
			a := make([]interface{}, len(columns))
			if t = t.Elem(); t == typeInterface && v.Len() > 0 {
				x := v.Index(0)
				n := x.Len()
				if n > len(a) {
					n = len(a)
				}
				for i := 0; i < n; i++ {
					a[i] = x.Index(i).Interface()
				}
			}
			return r.allSlice(columns, a, t, v)
		default:
			return r.allStruct(columns, newTableBy(t), v)
		}
	}
	panic("huge: type unsupported")
}
