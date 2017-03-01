// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"database/sql"
	"reflect"

	"github.com/cxr29/huge/query"
	"github.com/cxr29/log"
)

// Read *T returns bool, []T returns map[int]struct{}, map[]*T returns map[]struct{}.
func (h Huge) Read(i interface{}, columns ...string) (interface{}, error) {
	t := NewTable(i)
	v, _ := ptrElem(i)
	if t.PrimaryKey() == nil {
		panic(t.errNoPrimaryKey())
	}
	a := t.Filter(columns...)
	if a.Empty() {
		return nil, t.errNoColumns()
	}
	s := make([]*sql.Stmt, 2)
	defer func() {
		for _, j := range s {
			if j != nil {
				log.ErrWarning(j.Close())
			}
		}
	}()
	return h.read(s, t, a, v)
}

func (h Huge) read(s []*sql.Stmt, t *Table, a Columns, v reflect.Value) (_ interface{}, err error) {
	var b bool
	switch v.Kind() {
	case reflect.Map:
		m := reflect.MakeMap(reflect.MapOf(v.Type().Key(), typeEmpty))
		for _, i := range v.MapKeys() {
			b, err = h.read1(s, t, a, v.MapIndex(i))
			if err != nil {
				break
			}
			if b {
				m.SetMapIndex(i, zeroEmpty)
			}
		}
		return m.Interface(), err
	case reflect.Slice:
		n := v.Len()
		m := make(map[int]struct{}, n)
		for i := 0; i < n; i++ {
			b, err = h.read1(s, t, a, v.Index(i))
			if err != nil {
				break
			}
			if b {
				m[i] = struct{}{}
			}
		}
		return m, err
	}
	return h.read1(s, t, a, v)
}

func (h Huge) read1(s []*sql.Stmt, t *Table, a Columns, v reflect.Value) (_ bool, err error) {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return false, t.errNil()
		} else {
			v = v.Elem()
		}
	}
	b := make([]interface{}, len(a))
	f := make([]func() error, len(a))
	for i, c := range a {
		var ok bool
		b[i], f[i], ok = c.scan(v)
		if !ok {
			return false, c.errSet()
		}
	}
	p, _, err := t.getPrimaryKeyVersion(v)
	if err != nil {
		return
	}
	j := len(p) - 1
	if s[j] == nil {
		where := query.Where(t.PrimaryKey().Eq(1))
		if j == 1 {
			where.And(t.Version().Eq(2))
		}
		s[j], _, err = h.Prepare(query.Q(
			query.Select(a.Strings()...), query.From(t.Name), where,
		))
		if err != nil {
			return
		}
	}
	if err = s[j].QueryRow(p...).Scan(b...); err == ErrNoRows {
		return false, nil
	} else if err != nil {
		return
	}
	for _, i := range f {
		if i != nil {
			if err = i(); err != nil {
				return
			}
		}
	}
	return true, nil
}

// ReadBy PK returns *T, []PK returns []*T, map[PK] returns map[PK]*T or []*T only if without PK column.
func (h Huge) ReadBy(primaryKeys, row interface{}, columns ...string) (interface{}, error) {
	i, _, err := h.rud('r', primaryKeys, row, columns)
	return i, err
}
