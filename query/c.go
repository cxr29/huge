// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"bytes"
)

type Condition interface {
	Expression
	And(...Condition) Condition
	Not() Condition
	Or(...Condition) Condition
}

type condition struct {
	n bool
	e Expression
}

func (c condition) Expand(s Starter, i int) (q string, a []interface{}, err error) {
	q, a, err = Expand(c.e, false, s, i)
	if err == nil && c.n {
		q = "NOT (" + q + ")"
	}
	return
}

func (c condition) Not() Condition {
	return condition{!c.n, c.e}
}

func (c condition) And(a ...Condition) Condition {
	return logic1(false, c, a)
}

func (c condition) Or(a ...Condition) Condition {
	return logic1(true, c, a)
}

type logic struct {
	o bool
	a []Condition
}

func (l logic) Not() Condition {
	c := logic{!l.o, make([]Condition, len(l.a))}
	for k, v := range l.a {
		c.a[k] = v.Not()
	}
	return c
}

func (l logic) And(a ...Condition) Condition {
	if len(l.a) == 0 {
		return And(a...)
	} else if l.o {
		return logic1(true, l, a)
	}
	return logic2(false, l.a, a)
}

func (l logic) Or(a ...Condition) Condition {
	if len(l.a) == 0 {
		return Or(a...)
	} else if l.o {
		return logic2(true, l.a, a)
	}
	return logic1(false, l, a)
}

func newLogic(o bool, a []Condition) Condition {
	if len(a) == 1 {
		return a[0]
	}
	return logic{o, a}
}

func logic1(o bool, c Condition, a []Condition) Condition {
	b := make([]Condition, 1+len(a))
	b[0] = c
	copy(b[1:], a)
	return newLogic(o, b)
}

func logic2(o bool, a []Condition, b []Condition) Condition {
	c := make([]Condition, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return newLogic(o, c)
}

func (l logic) Expand(s Starter, i int) (string, []interface{}, error) {
	if len(l.a) == 0 {
		n := none("empty ")
		if l.o {
			n += "or"
		} else {
			n += "and"
		}
		return "", nil, n
	}
	var a []interface{}
	var b bytes.Buffer
	for k, v := range l.a {
		c, d, err := Expand(v, false, s, i+len(a))
		if err != nil {
			return "", nil, err
		}
		if k > 0 {
			if l.o {
				b.WriteString(" OR ")
			} else {
				b.WriteString(" AND ")
			}
		}
		b.WriteByte('(')
		b.WriteString(c)
		b.WriteByte(')')
		a = append(a, d...)
	}
	return b.String(), a, nil
}

func E2C(e Expression) Condition {
	if c, ok := e.(Condition); ok {
		return c
	}
	return condition{false, e}
}

func C(format string, a ...interface{}) Condition {
	return condition{false, E(format, a...)}
}

func Not(format string, a ...interface{}) Condition {
	return condition{true, E(format, a...)}
}

func And(a ...Condition) Condition {
	return newLogic(false, a)
}

func Or(a ...Condition) Condition {
	return newLogic(true, a)
}

type Logic struct {
	s string
	C Condition
}

func (l *Logic) Empty() bool {
	return l.C == nil
}

func (l *Logic) Not() *Logic {
	if l.C != nil {
		l.C = l.C.Not()
	}
	return l
}

func (l *Logic) And(a ...Condition) *Logic {
	if len(a) > 0 {
		if l.C != nil {
			l.C = l.C.And(a...)
		} else {
			l.C = And(a...)
		}
	}
	return l
}

func (l *Logic) Or(a ...Condition) *Logic {
	if len(a) > 0 {
		if l.C != nil {
			l.C = l.C.Or(a...)
		} else {
			l.C = Or(a...)
		}
	}
	return l
}

func (l *Logic) Expand(s Starter, i int) (q string, a []interface{}, err error) {
	if l.C != nil {
		q, a, err = Expand(l.C, false, s, i)
		if err == nil {
			q = l.s + q
		}
	}
	return
}

func L1(prefix string, a ...Condition) *Logic {
	l := &Logic{s: prefix}
	return l.And(a...)
}

func L(a ...Condition) *Logic {
	return L1("", a...)
}

func Where(a ...Condition) *Logic {
	return L1("WHERE ", a...)
}

func Having(a ...Condition) *Logic {
	return L1("HAVING ", a...)
}
