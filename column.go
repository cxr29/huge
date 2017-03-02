// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"reflect"
	"strings"

	"github.com/cxr29/huge/query"
)

const (
	oAutoIncrement = 1 << iota
	oAutoNow
	oAutoNowAdd
	oCollapse
	oForeignKey
	oGob
	oInline
	oInlineStatic
	oJSON
	oManyToMany
	oManyToOne
	oMap
	oOneToMany
	oOneToOne
	oPointer
	oPrimaryKey
	oScanner
	oValuer
	oVersion
	oXML
	//
	oNil
	oMany
	oOne
)

var options = map[string]struct {
	u uint
	b byte
	f func(reflect.Type) bool
}{
	"auto_increment": {oAutoIncrement, 'a', isIntegers},
	"auto_now":       {oAutoNow, 'a', isTimes},
	"auto_now_add":   {oAutoNowAdd, 'a', isTimes},
	"collapse":       {oCollapse, 'c', nil},
	"foreign_key":    {oForeignKey, 'r', isStruct},
	"gob":            {oGob, 'e', nil},
	"inline":         {oInline, 'i', isStruct},
	"inline_static":  {oInlineStatic, 'i', isStruct},
	"json":           {oJSON, 'e', nil},
	"many_to_many":   {oManyToMany, 'r', isStructs},
	"many_to_one":    {oManyToOne, 'r', isStruct},
	"one_to_many":    {oOneToMany, 'r', isStructs},
	"one_to_one":     {oOneToOne, 'r', isStruct},
	"primary_key":    {oPrimaryKey, 'p', nil},
	"version":        {oVersion, 'a', isIntegers},
	"xml":            {oXML, 'e', nil},
}

func option(u uint) string {
	for k, v := range options {
		if v.u == u {
			return k
		}
	}
	panic(false)
}

func parseOptions(t reflect.Type, s string) (e, n string, u uint) {
	if s == "-" {
		panic(false)
	}
	if t.Implements(typeScanner) {
		u |= oScanner
	}
	if t.Implements(typeValuer) {
		u |= oValuer
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		u |= oPointer
	}
	if len(s) == 0 {
		return
	}
	m := make(map[byte]string, 4)
	for k, v := range strings.Split(s, ",") {
		if k == 0 {
			n = v
		} else if o, ok := options[v]; !ok {
			e = fmt.Sprintf("unsupported option: %s", v)
			return
		} else if u&o.u == o.u {
			e = fmt.Sprintf("duplicate option %s", v)
			return
		} else if o.f != nil && !o.f(t) {
			e = fmt.Sprintf("type mismatch option %s", v)
			return
		} else if s, ok := m[o.b]; ok {
			e = fmt.Sprintf("option %s conflict with option %s", s, v)
			return
		} else {
			u |= o.u
			m[o.b] = v
		}
	}
	if s, ok := m['i']; ok && len(m) > 1 {
		e = fmt.Sprintf("option %s conflict with others", s)
		return
	}
	if u&oManyToMany == oManyToMany || u&oOneToMany == oOneToMany {
		if len(m) > 1 || len(n) > 0 {
			e = fmt.Sprintf("option %s conflict with others", m['r'])
			return
		}
		if t.Kind() == reflect.Map {
			u |= oMap
		}
	}
	return
}

type Field struct {
	t           reflect.Type
	o           uint
	i, j        int
	belong, own *Struct
	name, alias string
}

func (f *Field) Is(o uint) bool {
	return f.o&o == o
}

func (f *Field) IsAuto() bool {
	return f.Is(oAutoIncrement) || f.Is(oAutoNow) || f.Is(oAutoNowAdd) || f.Is(oVersion)
}

func (f *Field) IsEncoding() bool {
	return f.Is(oGob) || f.Is(oJSON) || f.Is(oXML)
}

func (f *Field) IsInline() bool {
	return f.Is(oInline) || f.Is(oInlineStatic)
}

func (f *Field) IsMany() bool {
	return f.Is(oManyToMany) || f.Is(oOneToMany)
}

func (f *Field) IsOne() bool {
	return f.Is(oForeignKey) || f.Is(oManyToOne) || f.Is(oOneToOne)
}

func (f *Field) CanNil() bool {
	return canNil(f.t.Kind())
}

func (f *Field) Type() reflect.Type {
	t := f.t
	if f.Is(oPointer) {
		t = t.Elem()
	}
	return t
}

type Column struct {
	t, r *Table
	i    int
	a    []*Field
	o    uint
	Name string
	query.Operand
}

func (c *Column) Qualifier(a ...string) query.Operand {
	return query.IQ(append(a, c.t.Name, c.Name)...)
}

func (c *Column) Rename(s string) {
	c.Name = s
	c.Operand = query.IQ(s)
}

func (c *Column) Clone(name string, collapse int) *Column {
	d := new(Column)
	*d = *c
	if len(name) > 0 {
		d.Rename(name)
	}
	if collapse > 0 {
		d.o |= oCollapse
	} else if collapse < 0 {
		d.o &^= oCollapse
	}
	return d
}

func (c *Column) first() *Field {
	return c.a[0]
}
func (c *Column) last() *Field {
	return c.a[len(c.a)-1]
}
func (c *Column) one() *Field {
	for _, f := range c.a {
		if f.IsOne() {
			return f
		}
	}
	return nil
}

func (c *Column) _isAutoIncrement() bool {
	i, ok := c.t.o[oAutoIncrement]
	return ok && i == c.i
}
func (c *Column) _isAutoNow() bool {
	i, ok := c.t.o[oAutoNow]
	return ok && i == c.i
}
func (c *Column) _isAutoNowAdd() bool {
	i, ok := c.t.o[oAutoNowAdd]
	return ok && i == c.i
}
func (c *Column) _isCollapse() bool {
	f := c.last()
	if f.CanNil() {
		return false
	}
	if c._isOne() {
		f = c.one()
	}
	return f.Is(oCollapse)
}
func (c *Column) _isMany() bool {
	return c.r != nil && c.last().IsMany()
}
func (c *Column) _isOne() bool {
	return c.r != nil && !c.last().IsMany()
}
func (c *Column) _isPrimaryKey() bool {
	i, ok := c.t.o[oPrimaryKey]
	return ok && i == c.i
}
func (c *Column) _isVersion() bool {
	i, ok := c.t.o[oVersion]
	return ok && i == c.i
}

func (c *Column) cache() {
	if c._isAutoIncrement() {
		c.o |= oAutoIncrement
	}
	if c._isAutoNow() {
		c.o |= oAutoNow
	}
	if c._isAutoNowAdd() {
		c.o |= oAutoNowAdd
	}
	if c._isCollapse() {
		c.o |= oCollapse
	}
	if c._isPrimaryKey() {
		c.o |= oPrimaryKey
	}
	if c._isVersion() {
		c.o |= oVersion
	}
	if c.last().CanNil() {
		c.o |= oNil
	}
	if c._isMany() {
		c.o |= oMany
	}
	if c._isOne() {
		c.o |= oOne
	}
}
func (c *Column) is(o uint) bool {
	return c.o&o == o
}
func (c *Column) isAutoIncrement() bool {
	return c.is(oAutoIncrement)
}
func (c *Column) isAutoNow() bool {
	return c.is(oAutoNow)
}
func (c *Column) isAutoNowAdd() bool {
	return c.is(oAutoNowAdd)
}
func (c *Column) isCollapse() bool {
	return c.is(oCollapse)
}
func (c *Column) isPrimaryKey() bool {
	return c.is(oPrimaryKey)
}
func (c *Column) isVersion() bool {
	return c.is(oVersion)
}
func (c *Column) canNil() bool {
	return c.is(oNil)
}
func (c *Column) isMany() bool {
	return c.is(oMany)
}
func (c *Column) isOne() bool {
	return c.is(oOne)
}

func (c *Column) field(v reflect.Value) (reflect.Value, bool) {
	if c.isMany() || v.Type() != c.t.s.t {
		panic(false)
	}
	if len(c.a) == 1 {
		return v.Field(c.a[0].i), true
	}
	for i, f := range c.a {
		if i > 0 {
			if f := c.a[i-1]; f.Is(oPointer) {
				if v.IsNil() {
					if v.CanSet() {
						v.Set(reflect.New(f.Type()))
					} else {
						return v, false
					}
				}
				v = v.Elem()
			}
		}
		v = v.Field(f.i)
	}
	return v, true
}
func (c *Column) scan(v reflect.Value) (interface{}, func() error, bool) {
	if v, ok := c.field(v); ok {
		if f := c.last(); !f.Is(oScanner) {
			if f.IsEncoding() {
				if ((c.isCollapse() || f.Is(oGob)) && !v.CanSet()) ||
					((f.Is(oJSON) || f.Is(oXML)) && !v.CanAddr()) {
					return nil, nil, false
				}
				var b []byte
				return &b, func() error {
					if c.isCollapse() && len(b) == 0 {
						v.Set(reflect.Zero(f.t))
						return nil
					} else if f.Is(oGob) {
						return gob.NewDecoder(bytes.NewReader(b)).DecodeValue(v)
					} else if f.Is(oJSON) {
						return json.Unmarshal(b, v.Addr().Interface())
					} else if f.Is(oXML) {
						return xml.Unmarshal(b, v.Addr().Interface())
					}
					panic(false)
				}, true
			} else if c.isCollapse() {
				if v.CanSet() {
					p := reflect.New(reflect.PtrTo(f.t))
					return p.Interface(), func() error {
						if p = p.Elem(); p.IsNil() {
							v.Set(reflect.Zero(f.t))
						} else {
							v.Set(p.Elem())
						}
						return nil
					}, true
				}
				return nil, nil, false
			}
		}
		if v.CanAddr() {
			return v.Addr().Interface(), nil, true
		}
	}
	return nil, nil, false
}
func (c *Column) scanNew() (interface{}, scanNewFunc) {
	f := c.last()
	if !f.Is(oScanner) {
		if f.IsEncoding() {
			var b []byte
			return &b, func() (_ reflect.Value, err error) {
				if c.isCollapse() && len(b) == 0 {
					return reflect.Zero(f.t), nil
				}
				v := reflect.New(f.t)
				if f.Is(oGob) {
					err = gob.NewDecoder(bytes.NewReader(b)).DecodeValue(v)
				} else if f.Is(oJSON) {
					err = json.Unmarshal(b, v.Interface())
				} else if f.Is(oXML) {
					err = xml.Unmarshal(b, v.Interface())
				} else {
					panic(false)
				}
				if err != nil {
					return
				}
				return v.Elem(), nil
			}
		} else if c.isCollapse() {
			p := reflect.New(reflect.PtrTo(f.t))
			return p.Interface(), func() (reflect.Value, error) {
				if p = p.Elem(); p.IsNil() {
					return reflect.Zero(f.t), nil
				}
				return p.Elem(), nil
			}
		}
	}
	return reflect.New(f.t).Interface(), nil
}
func (c *Column) get(v reflect.Value) (interface{}, error) {
	return c.getBy(true, true, v)
}
func (c *Column) getBy(collapse, encoding bool, v reflect.Value) (interface{}, error) {
	if v, ok := c.field(v); ok {
		return c.convert(collapse, encoding, v)
	} else if collapse && c.isCollapse() {
		return nil, nil
	}
	return nil, c.errGet()
}
func (c *Column) convert(collapse, encoding bool, v reflect.Value) (interface{}, error) {
	if f := c.last(); !f.Is(oValuer) {
		if collapse && c.isCollapse() && isZero(v) {
			return nil, nil
		} else if encoding && f.IsEncoding() {
			var b []byte
			var err error
			if f.Is(oGob) {
				var buf bytes.Buffer
				err = gob.NewEncoder(&buf).EncodeValue(v)
				b = buf.Bytes()
			} else if v.CanInterface() {
				if f.Is(oJSON) {
					b, err = json.Marshal(v.Interface())
				} else if f.Is(oXML) {
					b, err = xml.Marshal(v.Interface())
				} else {
					panic(false)
				}
			} else {
				return nil, c.errGet()
			}
			if err != nil {
				return nil, err
			} else if collapse && c.isCollapse() && len(b) == 0 {
				return nil, nil
			} else {
				return b, nil
			}
		}
	}
	if v.CanInterface() {
		return v.Interface(), nil
	}
	return nil, c.errGet()
}

func (c *Column) err(s string) error {
	return fmt.Errorf("huge: table %s column:%d %s: %s", c.t.Name, c.i+1, c.Name, s)
}
func (c *Column) errGet() error {
	return c.err("can not get")
}
func (c *Column) errSet() error {
	return c.err("can not set")
}
func (c *Column) errNil() error {
	return c.err("nil")
}
func (c *Column) errZero() error {
	return c.err("zero")
}
func (c *Column) errDuplicate() error {
	return c.err("duplicate")
}
