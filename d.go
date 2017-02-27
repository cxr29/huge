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

// Delete T returns bool, []T returns map[int]struct{}, map[]T returns map[]struct{}.
func (h Huge) Delete(i interface{}) (interface{}, error) {
	t := NewTable(i)
	v, _ := ptrElem(i)
	if t.PrimaryKey() == nil {
		panic(t.errNoPrimaryKey())
	}
	s := make([]*sql.Stmt, 2)
	defer func() {
		for _, i := range s {
			if i != nil {
				log.ErrWarning(i.Close())
			}
		}
	}()
	return h.remove(s, t, v)
}

func (h Huge) remove(s []*sql.Stmt, t *Table, v reflect.Value) (_ interface{}, err error) {
	var b bool
	switch v.Kind() {
	case reflect.Map:
		m := reflect.MakeMap(reflect.MapOf(v.Type().Key(), typeEmpty))
		for _, i := range v.MapKeys() {
			b, err = h.remove1(s, t, v.MapIndex(i))
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
			b, err = h.remove1(s, t, v.Index(i))
			if err != nil {
				break
			}
			if b {
				m[i] = struct{}{}
			}
		}
		return m, err
	}
	return h.remove1(s, t, v)
}

func (h Huge) remove1(s []*sql.Stmt, t *Table, v reflect.Value) (_ bool, err error) {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return false, t.errNil()
		} else {
			v = v.Elem()
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
			query.Delete(t.Name), where,
		))
		if err != nil {
			return
		}
	}
	r, err := s[j].Exec(p...)
	if err != nil {
		return
	}
	n, err := r.RowsAffected()
	if err != nil {
		return
	}
	if n == 0 {
		return false, nil
	} else if n == 1 {
		return true, nil
	}
	panic(fmt.Errorf("huge: RowsAffected expected 0 or 1 but was %d", n))
}

// DeleteBy PK, []PK, map[PK] returns the number of rows affected by delete.
func (h Huge) DeleteBy(primaryKeys, row interface{}) (int64, error) {
	_, i, err := h.rud('d', primaryKeys, row, nil)
	return i, err
}
