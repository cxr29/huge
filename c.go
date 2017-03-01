// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/cxr29/huge/query"
	"github.com/cxr29/log"
)

// Upsert if z is true, PK = 0 Create, PK > 0 Update, PK < 0 noop;
// otherwise if Read PK returns true then Update else Create.
func (h Huge) Upsert(z bool, i interface{}, columns ...string) (ok bool, err error) {
	t, a := newTable(i)
	if a[0] != nil || a[1] != nil {
		panic("huge: type unsupported")
	}
	c := t.PrimaryKey()
	if c == nil {
		panic(t.errNoPrimaryKey())
	}
	v, _ := ptrElem(i)
	if c := t.Version(); c != nil {
		if j, k := c.getInteger(v); !k || j >= 0 {
			return false, c.errGet()
		}
	}
	if z {
		if j, k := c.getInteger(v); !k {
			return false, c.errGet()
		} else if j < 0 {
			return
		} else {
			z = j > 0
		}
	} else if j, k := h.Read(i, c.Name); k != nil {
		return false, k
	} else {
		z = j.(bool)
	}
	if z {
		i, err = h.Update(i, columns...)
	} else {
		i, err = h.Create(i)
	}
	if i != nil {
		ok = i.(bool)
	}
	return
}

// Create T returns bool, []T returns int, map[]T returns map[]struct{}.
func (h Huge) Create(i interface{}) (interface{}, error) {
	t := NewTable(i)
	v, _ := ptrElem(i)
	values := query.X.Values()
	for _, c := range t.a {
		if c.isMany() || c.isAutoIncrement() {
			continue
		}
		values.Add(c.Name, values.Len()/2+1)
	}
	if values.Empty() {
		return nil, t.errNoColumns()
	}
	returning := false
	q := query.Q(query.Insert(t.Name), values)
	if c := t.AutoIncrement(); c != nil && h.ReturningFunc != nil {
		if r := h.ReturningFunc('c', c.Operand); r != nil {
			returning = true
			q.Append(r)
		}
	}
	s, _, err := h.Prepare(q)
	if err != nil {
		return nil, err
	}
	defer func() {
		log.ErrWarning(s.Close())
	}()
	return h.create(returning, s, t, v)
}

func (h Huge) create(returning bool, s *sql.Stmt, t *Table, v reflect.Value) (_ interface{}, err error) {
	now := time.Now()
	switch v.Kind() {
	case reflect.Map:
		m := reflect.MakeMap(reflect.MapOf(v.Type().Key(), typeEmpty))
		for _, i := range v.MapKeys() {
			err = h.create1(returning, s, t, v.MapIndex(i), now)
			if err != nil {
				break
			}
			m.SetMapIndex(i, zeroEmpty)
		}
		return m.Interface(), err
	case reflect.Slice:
		i := 0
		for n := v.Len(); i < n; i++ {
			err = h.create1(returning, s, t, v.Index(i), now)
			if err != nil {
				break
			}
		}
		return i, err
	}
	err = h.create1(returning, s, t, v, now)
	return err == nil, err
}

func (h Huge) create1(returning bool, s *sql.Stmt, t *Table, v reflect.Value, now time.Time) (err error) {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return t.errNil()
		}
		v = v.Elem()
	}
	a := make([]interface{}, 0, len(t.a))
	for _, c := range t.a {
		if c.isMany() || c.isAutoIncrement() {
			continue
		}
		var i interface{}
		if c.isVersion() {
			if i = c.convertInteger(1); i == nil {
				return c.errSet()
			}
		} else if c.isAutoNow() || c.isAutoNowAdd() {
			if i = c.convertTime(h.TimePrecision, now); i == nil {
				return c.errSet()
			}
		} else if i, err = c.get(v); err != nil {
			return
		}
		a = append(a, i)
	}
	set := func() error {
		if c := t.Version(); c != nil {
			if !c.setInteger(v, 1) {
				return c.errSet()
			}
		}
		if c := t.AutoNow(); c != nil {
			if !c.setTime(v, h.TimePrecision, now) {
				return c.errSet()
			}
		}
		if c := t.AutoNowAdd(); c != nil {
			if !c.setTime(v, h.TimePrecision, now) {
				return c.errSet()
			}
		}
		return nil
	}
	c := t.AutoIncrement()
	if returning {
		i, f, ok := c.scan(v)
		if !ok {
			return c.errSet()
		}
		err = s.QueryRow(a...).Scan(i)
		if err == nil && f != nil {
			err = f()
		}
		if err == nil {
			err = set()
		}
		return
	}
	r, err := s.Exec(a...)
	if err != nil {
		return
	}
	if c != nil {
		if i, err := r.LastInsertId(); err != nil {
			return err
		} else if !c.setInteger(v, i) {
			return c.errSet()
		}
	}
	n, err := r.RowsAffected()
	if err != nil {
		return
	}
	if n == 1 {
		return set()
	}
	panic(fmt.Errorf("huge: RowsAffected expected 1 but was %d", n))
}
