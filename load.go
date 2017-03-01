// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/cxr29/huge/query"
	"github.com/cxr29/log"
)

// Load T returns bool, []T returns int, map[]T returns map[]struct{}.
func (h Huge) Load(i interface{}) (interface{}, error) {
	t := NewTable(i)
	v, _ := ptrElem(i)
	values := query.X.Values()
	for _, c := range t.a {
		if c.isMany() {
			continue
		}
		values.Add(c.Name, values.Len()/2+1)
	}
	if values.Empty() {
		return nil, t.errNoColumns()
	}
	s, _, err := h.Prepare(query.Q(query.Insert(t.Name), values))
	if err != nil {
		return nil, err
	}
	defer func() {
		log.ErrWarning(s.Close())
	}()
	return h.load(s, t, v)
}

func (h Huge) load(s *sql.Stmt, t *Table, v reflect.Value) (_ interface{}, err error) {
	switch v.Kind() {
	case reflect.Map:
		m := reflect.MakeMap(reflect.MapOf(v.Type().Key(), typeEmpty))
		for _, i := range v.MapKeys() {
			err = h.load1(s, t, v.MapIndex(i))
			if err != nil {
				break
			}
			m.SetMapIndex(i, zeroEmpty)
		}
		return m.Interface(), err
	case reflect.Slice:
		i := 0
		for n := v.Len(); i < n; i++ {
			err = h.load1(s, t, v.Index(i))
			if err != nil {
				break
			}
		}
		return i, err
	}
	err = h.load1(s, t, v)
	return err == nil, err
}

func (h Huge) load1(s *sql.Stmt, t *Table, v reflect.Value) error {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return t.errNil()
		}
		v = v.Elem()
	}
	a := make([]interface{}, 0, len(t.a))
	for _, c := range t.a {
		if c.isMany() {
			continue
		}
		if i, err := c.get(v); err != nil {
			return err
		} else {
			a = append(a, i)
		}
	}
	r, err := s.Exec(a...)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	} else if n == 1 {
		return nil
	}
	panic(fmt.Errorf("huge: RowsAffected expected 1 but was %d", n))
}
