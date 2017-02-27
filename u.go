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

// Update T returns bool, []T returns map[int]struct{}, map[]T returns map[]struct{}.
func (h Huge) Update(i interface{}, columns ...string) (interface{}, error) {
	t := NewTable(i)
	v, _ := ptrElem(i)
	if t.PrimaryKey() == nil {
		panic(t.errNoPrimaryKey())
	}
	a := t.updateFilter(columns...)
	if a.Len() == 0 {
		return nil, t.errNoColumns()
	}
	var returning query.Expression
	if c := t.Version(); c != nil && h.ReturningFunc != nil {
		returning = h.ReturningFunc('u', c.Operand)
	}
	s := make([]*sql.Stmt, 2)
	defer func() {
		for _, j := range s {
			if j != nil {
				log.ErrWarning(j.Close())
			}
		}
	}()
	return h.update(returning, s, t, a, v)
}

func (h Huge) update(returning query.Expression, s []*sql.Stmt, t *Table, a Columns, v reflect.Value) (_ interface{}, err error) {
	now := time.Now()
	var b bool
	switch v.Kind() {
	case reflect.Map:
		m := reflect.MakeMap(reflect.MapOf(v.Type().Key(), typeEmpty))
		for _, i := range v.MapKeys() {
			b, err = h.update1(returning, s, t, a, v.MapIndex(i), now)
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
			b, err = h.update1(returning, s, t, a, v.Index(i), now)
			if err != nil {
				break
			}
			if b {
				m[i] = struct{}{}
			}
		}
		return m, err
	}
	return h.update1(returning, s, t, a, v, now)
}

func (h Huge) update1(returning query.Expression, s []*sql.Stmt, t *Table, a Columns, v reflect.Value, now time.Time) (_ bool, err error) {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return false, t.errNil()
		} else {
			v = v.Elem()
		}
	}
	b := make([]interface{}, 0, len(a)+1)
	for _, c := range a {
		if c.isVersion() {
			continue
		}
		var i interface{}
		if c.isAutoNow() {
			if i = c.convertTime(h.TimePrecision, now); i == nil {
				return false, c.errSet()
			}
		} else if i, err = c.get(v); err != nil {
			return
		}
		b = append(b, i)
	}
	p, i, err := t.getPrimaryKeyVersion(v)
	if err != nil {
		return
	}
	j := len(p) - 1
	b = append(b, p...)
	if s[j] == nil {
		set := query.X.Set()
		k := 0
		for _, c := range a {
			if c.isVersion() {
				set.Add(c.Name, c.Inc())
			} else {
				k++
				set.Add(c.Name, k)
			}
		}
		k++
		where := query.Where(t.PrimaryKey().Eq(k))
		if j == 1 {
			k++
			where.And(t.Version().Eq(k))
		}
		q := query.Q(query.Update(t.Name), set, where)
		if returning != nil {
			q.Append(returning)
		}
		s[j], _, err = h.Prepare(q)
		if err != nil {
			return
		}
	}
	if returning != nil {
		c := t.Version()
		k, f, ok := c.scan(v)
		if !ok {
			return false, c.errSet()
		}
		if err = s[j].QueryRow(b...).Scan(k); err == ErrNoRows {
			return false, nil
		} else if err == nil && f != nil {
			err = f()
		}
		if c = t.AutoNow(); err == nil && c != nil && !c.setTime(v, h.TimePrecision, now) {
			err = c.errSet()
		}
		return err == nil, err
	}
	r, err := s[j].Exec(b...)
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
		c := t.Version()
		if i > 0 && !c.setInteger(v, i+1) {
			return false, c.errSet()
		}
		if c = t.AutoNow(); c != nil && !c.setTime(v, h.TimePrecision, now) {
			return false, c.errSet()
		}
		return true, nil
	}
	panic(fmt.Errorf("huge: RowsAffected expected 0 or 1 but was %d", n))
}

// UpdateBy PK, []PK, map[PK] returns the number of rows affected by update.
func (h Huge) UpdateBy(primaryKeys, row interface{}, columns ...string) (int64, error) {
	_, i, err := h.rud('u', primaryKeys, row, columns)
	return i, err
}
