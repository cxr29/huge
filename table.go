// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cxr29/huge/query"
)

type Struct struct {
	t    reflect.Type
	a    []*Field
	name string
}

var structs = make(map[reflect.Type]*Struct)

func newStruct(t reflect.Type) (*Struct, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if isMapOrSlice(t.Kind()) {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	if t.Kind() != reflect.Struct {
		panic("huge: type unsupported")
	}
	s, ok := structs[t]
	if !ok {
		s = &Struct{t: t}
		structs[t] = s
		if err := s.fire(); err != nil {
			delete(structs, t)
			return nil, err
		}
	}
	return s, nil
}

func (s *Struct) fire() error {
	s.name = s.t.Name()
	for i, n := 0, s.t.NumField(); i < n; i++ {
		f := s.t.Field(i)
		if len(f.PkgPath) > 0 && !f.Anonymous {
			continue
		}
		t := f.Tag.Get("huge")
		if t == "-" {
			continue
		}
		e, t, o := parseOptions(f.Type, t)
		if len(e) > 0 {
			return fmt.Errorf("huge: struct %s field:%d %s: %s", s.name, i+1, f.Name, e)
		}
		v := &Field{t: f.Type, o: o, i: i, belong: s, name: f.Name, alias: t}
		if v.IsInline() || v.IsOne() || v.IsMany() {
			var err error
			if v.own, err = newStruct(v.t); err != nil {
				return err
			}
		}
		s.a = append(s.a, v)
		v.j = len(s.a)
	}
	return nil
}

func (s *Struct) bomb(m map[reflect.Type]*Table) (*Table, error) {
	t, ok := m[s.t]
	if ok {
		return t, nil
	}
	t = &Table{s: s}
	t.Rename(s.name)
	for _, f := range s.a {
		if f.IsInline() {
			fields := make([]*Field, 1, 5)
			fields[0] = f
			for a, i := f.own.a, 0; ; {
				for ; i < len(a); i++ {
					if a[i].IsInline() {
						for _, j := range fields {
							if j == a[i] || (j.own.t == a[i].own.t && j.i == a[i].i) {
								return nil, fmt.Errorf("huge: struct %s field:%d %s: inline circle", s.name, f.i+1, f.name)
							}
						}
						fields = append(fields, a[i])
						a = a[i].own.a
						i = 0
						continue
					} else {
						c := &Column{t: t, a: make([]*Field, len(fields)+1), i: len(t.a)}
						copy(c.a[:len(fields)], fields)
						c.a[len(fields)] = a[i]
						t.a = append(t.a, c)
					}
				}
				if j := len(fields) - 1; j == 0 {
					break
				} else {
					a = fields[j].own.a
					i = fields[j].j
					fields = fields[:j]
				}
			}
		} else {
			t.a = append(t.a, &Column{t: t, a: []*Field{f}, i: len(t.a)})
		}
	}
	a := make([]string, 0, 5)
	t.o = make(map[uint]int, 5)
	t.m = make(map[string]int, len(t.a))
	for _, c := range t.a {
		a = a[:0]
		static := false
		for j, f := range c.a {
			if (f.IsOne() || f.IsMany()) && j != len(c.a)-1 {
				panic(false)
			}
			if f.IsInline() && len(f.alias) > 0 {
				a = append(a, f.alias)
			}
			if f.Is(oInlineStatic) {
				static = true
			}
		}
		f := c.last()
		if f.IsInline() {
			panic(false)
		}
		if !static {
			for _, u := range [...]uint{oAutoIncrement, oAutoNow, oAutoNowAdd, oPrimaryKey, oVersion} {
				if f.Is(u) {
					if f.IsMany() {
						panic(false)
					}
					if _, ok := t.o[u]; ok {
						return nil, fmt.Errorf("huge: table %s: duplicate option %s", t.Name, option(u))
					} else {
						t.o[u] = c.i
					}
				}
			}
		}
		if f.IsOne() || f.IsMany() {
			r, err := newStruct(f.t)
			if err == nil {
				if m == nil {
					m = map[reflect.Type]*Table{s.t: t}
				} else {
					m[s.t] = t
				}
				c.r, err = r.bomb(m)
			}
			if err != nil {
				return nil, err
			}
		} else {
			if len(f.alias) > 0 {
				a = append(a, f.alias)
			} else {
				a = append(a, f.name)
			}
			c.Rename(strings.Join(a, ""))
			k := strings.ToLower(c.Name)
			if _, ok := t.m[k]; ok {
				return nil, fmt.Errorf("huge: table %s: duplicate column name: %s", t.Name, k)
			} else {
				t.m[k] = c.i
			}
		}
	}
	if i, ok := t.o[oPrimaryKey]; !ok {
		if i, ok = t.o[oAutoIncrement]; !ok {
			if i, ok = t.m["id"]; ok {
				t.o[oPrimaryKey] = i
				if isIntegers(t.a[i].last().Type()) {
					t.o[oAutoIncrement] = i
				}
			}
		} else {
			t.o[oPrimaryKey] = i
		}
	}
	for _, c := range t.a {
		if f := c.last(); f.IsOne() {
			for r, m := c.r, map[reflect.Type]struct{}{c.r.s.t: struct{}{}}; ; {
				if i, ok := r.o[oPrimaryKey]; !ok {
					return nil, fmt.Errorf("huge: table %s column:%d %s: table %s must have a primary key",
						t.Name, c.first().i+1, c.first().name, r.Name)
				} else {
					c.a = append(c.a, r.a[i].a...)
					if r.a[i].last().IsOne() {
						r = r.a[i].r
						if _, ok := m[r.s.t]; ok {
							for _, u := range [...]uint{oForeignKey, oManyToOne, oOneToOne} {
								if f.Is(u) {
									return nil, fmt.Errorf("huge: table %s column:%d %s: %s circle",
										t.Name, c.first().i+1, c.first().name, option(u))
								}
							}
							panic(false)
						} else {
							m[r.s.t] = struct{}{}
						}
					} else {
						break
					}
				}
			}
			a = a[:0]
			if len(f.alias) > 0 {
				a = append(a, f.alias)
				f = c.last()
			} else {
				a = append(a, c.r.s.name)
				f = c.last()
				if len(f.alias) > 0 {
					a = append(a, f.alias)
				} else {
					a = append(a, f.name)
				}
			}
			if f.IsOne() || f.IsMany() || f.IsInline() {
				panic(false)
			}
			c.Rename(strings.Join(a, ""))
			k := strings.ToLower(c.Name)
			if _, ok := t.m[k]; ok {
				return nil, fmt.Errorf("huge: table %s: duplicate column name: %s", t.Name, k)
			} else {
				t.m[k] = c.i
			}
		}
	}
	for _, c := range t.a {
		c.cache()
	}
	return t, nil
}

var tables = make(map[reflect.Type]*Table)

func ptrElem(i interface{}) (v reflect.Value, p bool) {
	if i == nil {
		panic("huge: nil")
	}
	v = reflect.ValueOf(i)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			panic("huge: nil pointer")
		} else {
			v = v.Elem()
			p = true
		}
	}
	return
}
func NewTable(i interface{}) *Table {
	t, _ := newTable(i)
	return t
}
func newTable(i interface{}) (_ *Table, a [2]reflect.Type) {
	if i == nil {
		panic("huge: nil")
	}
	t := reflect.TypeOf(i)
	k := t.Kind()
	f1 := func() {
		t = t.Elem()
		k = t.Kind()
	}
	f2 := func() {
		if k == reflect.Ptr {
			f1()
		}
	}
	switch f2(); k {
	case reflect.Map:
		a[0] = t
		f1()
		f2()
	case reflect.Slice:
		a[1] = t
		f1()
		f2()
	}
	if k == reflect.Struct {
		return newTableBy(t), a
	}
	panic("huge: type unsupported")
}
func newTableBy(t reflect.Type) *Table {
	s, err := newStruct(t)
	if err != nil {
		panic(err)
	}
	v, ok := tables[s.t]
	if !ok {
		v, err = s.bomb(nil)
		if err != nil {
			panic(err)
		}
		tables[s.t] = v
	}
	return v
}

type Table struct {
	s    *Struct
	a    []*Column
	o    map[uint]int
	m    map[string]int
	Name string
	query.Operand
}

func (t *Table) Qualifier(a ...string) query.Operand {
	return query.IQ(append(a, t.Name)...)
}

func (t *Table) Rename(s string) {
	t.Name = s
	t.Operand = query.IQ(s)
}

func (t *Table) AutoIncrement() *Column {
	if i, ok := t.o[oAutoIncrement]; ok {
		return t.a[i]
	}
	return nil
}
func (t *Table) AutoNow() *Column {
	if i, ok := t.o[oAutoNow]; ok {
		return t.a[i]
	}
	return nil
}
func (t *Table) AutoNowAdd() *Column {
	if i, ok := t.o[oAutoNowAdd]; ok {
		return t.a[i]
	}
	return nil
}
func (t *Table) PrimaryKey() *Column {
	if i, ok := t.o[oPrimaryKey]; ok {
		return t.a[i]
	}
	return nil
}
func (t *Table) Version() *Column {
	if i, ok := t.o[oVersion]; ok {
		return t.a[i]
	}
	return nil
}

func (t *Table) Find(s string) *Column {
	if i, ok := t.m[strings.ToLower(s)]; ok {
		return t.a[i]
	}
	return nil
}

const Exclude = ""

type Columns []*Column

func (a Columns) Len() int {
	return len(a)
}

func (a Columns) Strings() (b []string) {
	if len(a) > 0 {
		b = make([]string, len(a))
		for i, c := range a {
			b[i] = c.Name
		}
	}
	return
}
func (a Columns) Slice() (b []interface{}) {
	if len(a) > 0 {
		b = make([]interface{}, len(a))
		for i, c := range a {
			b[i] = c
		}
	}
	return
}
func (a Columns) Map() (m map[string]interface{}) {
	if len(a) > 0 {
		m = make(map[string]interface{}, len(a))
		for _, c := range a {
			m[c.Name] = c
		}
	}
	return
}

func (t *Table) Filter(columns ...string) Columns {
	exclude := len(columns) > 0 && columns[0] == Exclude
	if exclude {
		columns = columns[1:]
	}
	a := make(Columns, 0, len(t.a))
	if len(columns) == 0 {
		for _, c := range t.a {
			if c.isMany() {
				continue
			}
			a = append(a, c)
		}
	} else {
		var i int
		var ok bool
		a = a[:len(t.a)]
		for _, s := range columns {
			k := strings.ToLower(s)
			if i, ok = t.m[k]; ok {
				a[i] = t.a[i]
			} else {
				panic("huge: column not found: " + s)
			}
		}
		i = 0
		for j := 0; j < len(t.a); j++ {
			if exclude {
				ok = a[j] == nil && !t.a[j].isMany()
			} else {
				ok = a[j] != nil
			}
			if ok {
				a[i] = t.a[j]
				i++
			}
		}
		a = a[:i]
	}
	return a
}

func (t *Table) updateFilter(columns ...string) Columns {
	exclude := len(columns) > 0 && columns[0] == Exclude
	if exclude {
		columns = columns[1:]
	}
	a := make(Columns, 0, len(t.a))
	if len(columns) == 0 {
		for _, c := range t.a {
			if c.isMany() || c.isAutoIncrement() || c.isAutoNowAdd() || c.isPrimaryKey() {
				continue
			}
			a = append(a, c)
		}
	} else {
		m := make(map[int]struct{}, len(columns))
		for _, s := range columns {
			s = strings.ToLower(s)
			if i, ok := t.m[s]; ok {
				m[i] = struct{}{}
			}
		}
		for _, c := range t.a {
			if c.isMany() {
				continue
			}
			ok := c.isAutoNow() || c.isVersion()
			if !ok {
				if _, ok = m[c.i]; exclude {
					ok = !ok
				}
			}
			if ok {
				a = append(a, c)
			}
		}
	}
	return a
}

func (t *Table) getVersion(v reflect.Value) (int64, interface{}, error) {
	if c := t.Version(); c != nil {
		if i, ok := c.getInteger(v); !ok {
			return 0, nil, c.errGet()
		} else if i == 0 {
			return 0, nil, c.errZero()
		} else if i > 0 {
			if j, err := c.get(v); err != nil {
				return 0, nil, err
			} else {
				return i, j, err
			}
		}
	}
	return 0, nil, nil
}

func (t *Table) getPrimaryKeyVersion(v reflect.Value) (a []interface{}, i int64, err error) {
	a = make([]interface{}, 2)
	a[0], err = t.PrimaryKey().get(v)
	if err == nil {
		i, a[1], err = t.getVersion(v)
	}
	if err != nil {
		a = a[:0]
	} else if i <= 0 {
		a = a[:1]
	}
	return
}

func (t *Table) err(s string) error {
	return fmt.Errorf("huge: table %s: %s", t.Name, s)
}
func (t *Table) errNil() error {
	return t.err("nil")
}
func (t *Table) errNoColumns() error {
	return t.err("no columns")
}
func (t *Table) errNoPrimaryKey() error {
	return t.err("no primary key")
}
