// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"database/sql"
	"reflect"
	"time"

	"github.com/cxr29/huge/query"
	"github.com/cxr29/log"
)

func (h Huge) rud(b byte, primaryKeys, row interface{}, columns []string) (_ interface{}, n int64, err error) {
	t, types := newTable(row)
	if types[0] != nil || types[1] != nil {
		panic("huge: type unsupported")
	}
	v, _ := ptrElem(row)
	if primaryKeys == nil {
		panic("huge: nil")
	}
	c := t.PrimaryKey()
	if c == nil {
		panic(t.errNoPrimaryKey())
	}
	f := c.last()
	var cols Columns
	switch b {
	case 'r':
		cols = t.Filter(columns...)
		if cols.Empty() {
			return nil, 0, t.errNoColumns()
		}
	case 'u':
		cols = t.updateFilter(columns...)
		if cols.Empty() {
			return nil, 0, t.errNoColumns()
		}
	}
	i, j, err := t.getVersion(v)
	if err != nil {
		return
	}
	var a []interface{}
	{
		v := reflect.ValueOf(primaryKeys)
		t := v.Type()
		if t.Kind() == reflect.Ptr && f.t != t {
			if v.IsNil() {
				panic("huge: nil pointer")
			}
			v = v.Elem()
			t = v.Type()
		}
		switch t.Kind() {
		case reflect.Map:
			if f.t != t.Key() {
				panic("huge: type unsupported")
			}
			if b == 'r' {
				n = 1
				for _, c := range cols {
					if c.isPrimaryKey() {
						n = 2
						break
					}
				}
			}
			a = make([]interface{}, v.Len())
			for i, k := range v.MapKeys() {
				a[i], err = c.convert(true, true, k)
				if err != nil {
					break
				}
			}
		case reflect.Slice:
			if f.t != t.Elem() {
				panic("huge: type unsupported")
			}
			if b == 'r' {
				n = 1
			}
			a = make([]interface{}, v.Len())
			for i := range a {
				a[i], err = c.convert(true, true, v.Index(i))
				if err != nil {
					break
				}
			}
		default:
			if f.t != t {
				panic("huge: type unsupported")
			}
			a = make([]interface{}, 1)
			a[0], err = c.convert(true, true, v)
		}
	}
	if err != nil {
		return
	} else if len(a) == 0 {
		if b == 'r' {
			switch n {
			case 0:
				v = reflect.Zero(reflect.PtrTo(t.s.t))
			case 1:
				v = reflect.Zero(reflect.SliceOf(reflect.PtrTo(t.s.t)))
			case 2:
				v = reflect.Zero(reflect.MapOf(f.t, reflect.PtrTo(t.s.t)))
			default:
				panic(false)
			}
			return v.Interface(), n, nil
		} else {
			return
		}
	}
	where := query.Where()
	if len(a) == 1 {
		where.And(c.Eq(a[0]))
	} else {
		where.And(c.In(a...))
	}
	if i > 0 {
		where.And(t.Version().Eq(j))
	}
	switch b {
	case 'r':
		var s string
		s, a, err = h.Expand(query.Q(
			query.Select(cols.Strings()...), query.From(t.Name), where,
		))
		if err != nil {
			return
		}
		var r *sql.Rows
		r, err = h.querier.Query(s, a...)
		if err != nil {
			return
		}
		defer func() {
			log.ErrWarning(r.Close())
		}()
		switch n {
		case 0:
			v = reflect.Zero(reflect.PtrTo(t.s.t))
		case 1:
			v = reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(t.s.t)), 0, len(a))
		case 2:
			v = reflect.MakeMap(reflect.MapOf(f.t, reflect.PtrTo(t.s.t)))
		default:
			panic(false)
		}
		b := make([]interface{}, len(cols))
		f := make([]func() error, len(cols))
	Loop:
		for r.Next() {
			p := reflect.New(t.s.t)
			q := p.Elem()
			for i, c := range cols {
				var ok bool
				b[i], f[i], ok = c.scan(q)
				if !ok {
					err = c.errSet()
					break Loop
				}
			}
			if err = r.Scan(b...); err != nil {
				break
			}
			for _, i := range f {
				if i != nil {
					if err = i(); err != nil {
						break
					}
				}
			}
			switch n {
			case 0:
				v = p
				break
			case 1:
				v = reflect.Append(v, p)
			case 2:
				if k, ok := c.field(q); ok {
					if v.MapIndex(k).IsValid() {
						err = c.errDuplicate()
						break
					} else {
						v.SetMapIndex(k, p)
					}
				} else {
					err = c.errGet()
					break
				}
			}
		}
		if err == nil {
			err = r.Err()
		}
		if err == nil {
			err = r.Close()
		}
		return v.Interface(), n, err
	case 'u':
		set, now := query.X.Set(), time.Now()
		for _, c := range cols {
			if c.isVersion() {
				set.Add(c.Name, c.Inc())
			} else {
				var i interface{}
				if c.isAutoNow() {
					if i = c.convertTime(h.TimePrecision, now); i == nil {
						return nil, 0, c.errSet()
					}
				} else if i, err = c.get(v); err != nil {
					return
				}
				set.Add(c.Name, i)
			}
		}
		var r sql.Result
		r, err = h.Exec(query.Q(
			query.Update(t.Name), set, where,
		))
		if err == nil {
			if n, err = r.RowsAffected(); err == nil && n > 0 {
				c := t.Version()
				if i > 0 && !c.setInteger(v, i+1) {
					return nil, 0, c.errSet()
				}
				if c = t.AutoNow(); c != nil && !c.setTime(v, h.TimePrecision, now) {
					return nil, 0, c.errSet()
				}
			}
		}
		return
	case 'd':
		var r sql.Result
		r, err = h.Exec(query.Q(
			query.Delete(t.Name), where,
		))
		if err == nil {
			n, err = r.RowsAffected()
		}
		return
	}
	panic(false)
}
