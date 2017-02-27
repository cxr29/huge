// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type none string

func (e none) Error() string {
	return string(e)
}

func (e none) Expand(int, ParameterFunc, QuotationFunc) (string, []interface{}, error) {
	return "", nil, e
}

func nonef(format string, a ...interface{}) none {
	return none(fmt.Sprintf(format, a...))
}

type empty string

func (e empty) Expand(int, ParameterFunc, QuotationFunc) (string, []interface{}, error) {
	return "", nil, nil
}

type value struct {
	i interface{}
}

func (v value) Expand(n int, p ParameterFunc, q QuotationFunc) (s string, a []interface{}, err error) {
	fp := Parameter
	if p != nil {
		fp = p
	}
	s = fp(n, true)
	if len(s) == 0 {
		s = fp(n, false)
	}
	if len(s) == 0 {
		err = fmt.Errorf("unsupported parameter:%d: %v", n, v.i)
	} else {
		a = []interface{}{v.i}
	}
	return
}

type Literal string

func (e Literal) Expand(int, ParameterFunc, QuotationFunc) (string, []interface{}, error) {
	return string(e), nil, nil
}
func (e Literal) And(a ...Condition) Condition {
	return condition{e: e}.And(a...)
}
func (e Literal) Not() Condition {
	return condition{true, e}
}
func (e Literal) Or(a ...Condition) Condition {
	return condition{e: e}.Or(a...)
}

func Literalf(format string, a ...interface{}) Literal {
	return Literal(fmt.Sprintf(format, a...))
}

type Identifier string

func (e Identifier) Expand(n int, p ParameterFunc, q QuotationFunc) (s string, _ []interface{}, err error) {
	fq := Quotation
	if q != nil {
		fq = q
	}
	s = fq(Quote(string(e), '"'))
	if len(s) == 0 {
		err = errors.New("unsupported identifier: " + string(e))
	}
	return
}

type Qualifier []string

func (e Qualifier) Expand(n int, p ParameterFunc, q QuotationFunc) (string, []interface{}, error) {
	if len(e) == 0 {
		return "", nil, errors.New("empty qualifier")
	}
	fq := Quotation
	if q != nil {
		fq = q
	}
	a := make([]string, len(e))
	for k, v := range e {
		v = fq(Quote(string(v), '"'))
		if len(v) == 0 {
			return "", nil, fmt.Errorf("unsupported qualifier: %v:%d", e, k)
		}
		a[k] = v
	}
	return strings.Join(a, "."), nil, nil
}

type expression struct {
	s string
	a []interface{}
}

func (e expression) Expand(n int, p ParameterFunc, q QuotationFunc) (s string, a []interface{}, err error) {
	var b bytes.Buffer
	var i, j, k, l int
	m := make(map[int]int, len(e.a))
	fp := Parameter
	if p != nil {
		fp = p
	}
	fq := Quotation
	if q != nil {
		fq = q
	}
	for j < len(e.s) {
		k = strings.IndexByte(meta, e.s[j])
		if k == -1 {
			b.WriteByte(e.s[j])
			j++
			continue
		}
		l = j + 1
		switch k {
		case 0, 2, 4:
			for l < len(e.s) {
				if e.s[l] == e.s[j] {
					l++
					if l < len(e.s) && e.s[l] == e.s[j] {
						l++
					} else {
						s = fq(e.s[j:l])
						j = l
						break
					}
				} else {
					l++
				}
			}
			if j == l {
				if len(s) == 0 {
					return "", nil, fmt.Errorf("unsupported %s: %s:%d %v", marks[k], e.s, j, e.a)
				} else {
					b.WriteString(s)
				}
			} else {
				return "", nil, fmt.Errorf("unclosed %s: %s:%d %v", marks[k], e.s, j, e.a)
			}
		case 1, 3:
			for l < len(e.s) {
				if '0' <= e.s[l] && e.s[l] <= '9' {
					l++
				} else {
					break
				}
			}
			h := j + 1
			if h == l {
				i++
				h = i
			} else if e.s[h] == '0' {
				return "", nil, fmt.Errorf("leading zero: %s:%d %v", e.s, j, e.a)
			} else {
				h, err = strconv.Atoi(e.s[h:l])
			}
			if err != nil || h < 1 {
				return "", nil, fmt.Errorf("out of range: %s:%d %v", e.s, j, e.a)
			} else if h > len(e.a) {
				return "", nil, fmt.Errorf("too few arguments: %s:%d %v:%d", e.s, j, e.a, h)
			}
			h--
			if _, ok := m[h]; !ok {
				m[h] = len(a)
			}
			if v, ok := e.a[h].(Expression); ok {
				var d []interface{}
				s, d, err = Expand(v, false, n+len(a), p, q)
				if err != nil {
					return "", nil, err
				}
				b.WriteString(s)
				a = append(a, d...)
			} else {
				s = fp(n+m[h], true)
				if len(s) == 0 {
					s = fp(n+len(a), false)
				} else {
					ok = m[h] != len(a)
				}
				if len(s) == 0 {
					return "", nil, fmt.Errorf("unsupported %s parameter:%d: %s:%d %v", marks[k], n+len(a), e.s, j, e.a)
				}
				b.WriteString(s)
				if !ok {
					a = append(a, e.a[h])
				}
			}
			j = l
		default:
			panic(k)
		}
	}
	if len(m) < len(e.a) {
		return "", nil, fmt.Errorf("too many arguments: %s %v:%d", e.s, e.a, len(m))
	} else if len(m) > len(e.a) {
		panic(false)
	}
	return b.String(), a, nil
}

func Expand(e Expression, omitempty bool, n int, p ParameterFunc, q QuotationFunc) (s string, a []interface{}, err error) {
	if e == nil {
		err = errors.New("nil expression")
	} else if s, a, err = e.Expand(n, p, q); err == nil {
		if len(s) < len(a) {
			err = fmt.Errorf("malformed expression: %v", e)
		} else if len(s) == 0 && !omitempty {
			err = fmt.Errorf("empty expression: %v", e)
		}
	}
	return
}

func V2E(i interface{}) Expression {
	if e, ok := i.(Expression); ok {
		return e
	}
	return value{i}
}

func E(s string, a ...interface{}) Expression {
	return expression{s: s, a: a}
}
