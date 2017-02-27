// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"time"
)

const (
	Interface Kind = iota
	Bool
	Int
	Int8
	Int16
	Int32
	Int64
	Uint
	Uint8
	Uint16
	Uint32
	Uint64
	Float32
	Float64
	String
	Time
	Bytes
)

var (
	typeEmpty     = reflect.TypeOf(struct{}{})
	typeString    = reflect.TypeOf("")
	typeTime      = reflect.TypeOf(time.Time{})
	typeInterface = reflect.TypeOf(([]interface{})(nil)).Elem()
	typeScanner   = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	typeValuer    = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	types         = map[Kind]reflect.Type{
		Bool:    reflect.TypeOf(false),
		Int:     reflect.TypeOf(int(0)),
		Int8:    reflect.TypeOf(int8(0)),
		Int16:   reflect.TypeOf(int16(0)),
		Int32:   reflect.TypeOf(int32(0)),
		Int64:   reflect.TypeOf(int64(0)),
		Uint:    reflect.TypeOf(uint(0)),
		Uint8:   reflect.TypeOf(uint8(0)),
		Uint16:  reflect.TypeOf(uint16(0)),
		Uint32:  reflect.TypeOf(uint32(0)),
		Uint64:  reflect.TypeOf(uint64(0)),
		Float32: reflect.TypeOf(float32(0)),
		Float64: reflect.TypeOf(float64(0)),
		String:  typeString,
		Time:    typeTime,
	}
)

var zeroEmpty = reflect.Zero(typeEmpty)

type scanNewFunc func() (reflect.Value, error)

type Kind int

func (k Kind) scanNew() (interface{}, scanNewFunc) {
	p := k < 0
	if p {
		k = -k
	}
	if t, ok := types[k]; ok {
		if p {
			v := reflect.New(reflect.PtrTo(t))
			return v.Interface(), func() (reflect.Value, error) {
				v = v.Elem()
				if v.IsNil() {
					return reflect.Zero(t), nil
				}
				return v.Elem(), nil
			}
		}
		return reflect.New(t).Interface(), nil
	}
	switch k {
	case Interface:
		var i interface{}
		return &i, nil
	case Bytes:
		var b []byte
		return &b, nil
	}
	panic("huge: type unsupported")
}

func scanNew(i interface{}) (interface{}, scanNewFunc) {
	switch x := i.(type) {
	case nil:
		var j interface{}
		return &j, nil
	case Kind:
		return x.scanNew()
	case *Column:
		return x.scanNew()
	case *Kind, Column, Table, *Table, Columns, *Columns:
		panic("huge: type unsupported")
	}
	return reflect.New(reflect.TypeOf(i)).Interface(), nil
}
