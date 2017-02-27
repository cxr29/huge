// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"bytes"
)

type condition struct {
	n bool
	e Expression
}

func (c condition) Expand(n int, p ParameterFunc, q QuotationFunc) (string, []interface{}, error) {
	s, a, err := Expand(c.e, false, n, p, q)
	if err == nil && c.n {
		s = "NOT (" + s + ")"
	}
	return s, a, err
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
	if len(l.a) == 0 {
		return logic{o: !l.o}
	}
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

func (l logic) Expand(n int, p ParameterFunc, q QuotationFunc) (string, []interface{}, error) {
	var a []interface{}
	var b bytes.Buffer
	for k, v := range l.a {
		c, d, err := Expand(v, false, n+len(a), p, q)
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
	c Condition
}

func (l *Logic) Not() *Logic {
	l.c = l.c.Not()
	return l
}

func (l *Logic) And(a ...Condition) *Logic {
	l.c = l.c.And(a...)
	return l
}

func (l *Logic) Or(a ...Condition) *Logic {
	l.c = l.c.Or(a...)
	return l
}

func (l *Logic) Expand(n int, p ParameterFunc, q QuotationFunc) (string, []interface{}, error) {
	s, a, err := Expand(l.c, true, n, p, q)
	if err == nil && len(s) > 0 {
		s = l.s + s
	}
	return s, a, err
}

func Where(a ...Condition) *Logic {
	return &Logic{"WHERE ", And(a...)}
}

func Having(a ...Condition) *Logic {
	return &Logic{"HAVING ", And(a...)}
}

func On(a ...Condition) *Logic {
	return &Logic{"ON ", And(a...)}
}

func Using(a ...string) Expression {
	if len(a) == 0 {
		return empty("USING")
	}
	return Q3("USING (", ", ", ")").Add(a...)
}
