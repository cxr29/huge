// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"bytes"
)

type Query struct {
	b byte
	s [3]string
	a []Expression
}

func (e *Query) Empty() bool {
	return len(e.a) == 0
}

func (e *Query) Len() int {
	return len(e.a)
}

func (e *Query) Append(a ...Expression) *Query {
	e.a = append(e.a, a...)
	return e
}

func (e *Query) Add(a ...string) *Query {
	for _, s := range a {
		var i Expression = Identifier(s)
		if e.b == 'o' && len(s) > 0 {
			switch s[0] {
			case '+':
				i = Asc(s[1:])
			case '-':
				i = Desc(s[1:])
			}
		}
		e.a = append(e.a, i)
	}
	return e
}

func (e *Query) As(s string) Expression {
	return E("(?) AS ?", e, Identifier(s))
}

func (e *Query) Expand(s Starter, i int) (q string, a []interface{}, _ error) {
	if e.b == '*' && len(e.a) == 0 {
		return e.s[0] + "*" + e.s[2], nil, nil
	}
	o := e.b == 't' || e.b == 'o'
	var b bytes.Buffer
	var k int
	b.WriteString(e.s[0])
	for _, v := range e.a {
		c, d, err := Expand(v, o, s, i+len(a))
		if err != nil {
			return "", nil, err
		} else if len(c) == 0 {
			continue
		}
		if k > 0 {
			b.WriteString(e.s[1])
		}
		b.WriteString(c)
		a = append(a, d...)
		k++
	}
	if k > 0 {
		b.WriteString(e.s[2])
		q = b.String()
	} else if !o {
		return "", nil, nonef("empty query3: %v", e)
	}
	return
}

func q2(operator string, a, b Expression) Expression {
	return Q3("", " "+operator+" ", "", a, b)
}

func Q3(prefix, delimiter, suffix string, a ...Expression) *Query {
	return &Query{'f', [...]string{prefix, delimiter, suffix}, a}
}

func Q3Star(prefix, delimiter, suffix string, a ...Expression) *Query {
	return &Query{'*', [...]string{prefix, delimiter, suffix}, a}
}

func Q3Empty(prefix, delimiter, suffix string, a ...Expression) *Query {
	return &Query{'t', [...]string{prefix, delimiter, suffix}, a}
}

func Q(a ...Expression) *Query {
	return Q3Empty("", " ", "", a...)
}

type QueryS struct {
	o bool
	s []string
	a []interface{}
}

func (e *QueryS) Empty() bool {
	return len(e.a) == 0
}

func (e *QueryS) Len() int {
	return len(e.a)
}

func (e *QueryS) Append(a ...interface{}) *QueryS {
	if !e.o {
		e.o = len(a)%2 == 1
	}
	e.a = append(e.a, a...)
	return e
}

func (e *QueryS) Add(c string, i interface{}) *QueryS {
	e.a = append(e.a, Identifier(c), i)
	return e
}

func (e *QueryS) Expand(s Starter, i int) (_ string, _ []interface{}, err error) {
	if e.o {
		err = nonef("odd query4: %v:%d", e, len(e.a))
	} else if len(e.a) == 0 {
		err = nonef("empty query4: %v", e)
	} else if len(e.s) == 4 {
		return e.expandS1(s, i)
	} else if len(e.s) == 5 {
		return e.expandS2(s, i)
	} else {
		err = nonef("malformed query4: %v", e)
	}
	return
}

func (e *QueryS) expandS1(s Starter, i int) (string, []interface{}, error) {
	var a []interface{}
	var b bytes.Buffer
	b.WriteString(e.s[0])
	for k, v := range e.a {
		c, d, err := Expand(V2E(v), false, s, i+len(a))
		if err != nil {
			return "", nil, err
		}
		if k%2 == 1 {
			b.WriteString(e.s[1])
		} else if k > 0 {
			b.WriteString(e.s[2])
		}
		b.WriteString(c)
		a = append(a, d...)
	}
	b.WriteString(e.s[3])
	return b.String(), a, nil
}

func (e *QueryS) expandS2(s Starter, i int) (_ string, _ []interface{}, err error) {
	var a []interface{}
	var b bytes.Buffer
	f := func(j int, q string) error {
		for ; j < len(e.a); j += 2 {
			c, d, err := Expand(V2E(e.a[j]), false, s, i+len(a))
			if err != nil {
				return err
			}
			if j >= 2 {
				b.WriteString(q)
			}
			b.WriteString(c)
			a = append(a, d...)
		}
		return nil
	}
	b.WriteString(e.s[0])
	if err = f(0, e.s[1]); err != nil {
		return
	}
	b.WriteString(e.s[2])
	if err = f(1, e.s[3]); err != nil {
		return
	}
	b.WriteString(e.s[4])
	return b.String(), a, nil
}

func Q3S1(prefix, separater1, delimiter, suffix string, a ...interface{}) *QueryS {
	return &QueryS{len(a)%2 == 1, []string{prefix, separater1, delimiter, suffix}, a}
}

func Q3S2(prefix, separater1, delimiter, separater2, suffix string, a ...interface{}) *QueryS {
	return &QueryS{len(a)%2 == 1, []string{prefix, separater1, delimiter, separater2, suffix}, a}
}

func Insert(s string) Expression {
	return E("INSERT INTO ?", Identifier(s))
}

func Values(c string, i interface{}) *QueryS {
	return X.Values().Add(c, i)
}

func Update(s string) Expression {
	return E("UPDATE ?", Identifier(s))
}

func Set(c string, i interface{}) *QueryS {
	return X.Set().Add(c, i)
}

func Delete(s string) Expression {
	return E("DELETE FROM ?", Identifier(s))
}

func SelectCount() Expression {
	return Literal("SELECT COUNT(*)")
}

func SelectDistinct(a ...string) *Query {
	return Q3Star("SELECT DISTINCT ", ", ", "").Add(a...)
}

func Select(a ...string) *Query {
	return Q3Star("SELECT ", ", ", "").Add(a...)
}

func From(a ...string) *Query {
	return Q3("FROM ", ", ", "").Add(a...)
}

// OrderBy +ASC, -DESC.
func OrderBy(a ...string) *Query {
	return X.OrderBy().Add(a...)
}

func GroupBy(a ...string) *Query {
	return Q3Empty("GROUP BY ", ", ", "").Add(a...)
}

func Limit(a ...int) Expression {
	switch len(a) {
	case 0:
		return empty("LIMIT")
	case 1:
		return limit(a[0])
	case 2:
		if a[0] < 0 {
			return limit(a[1])
		} else if a[1] < 0 {
			return Offset(a[0])
		} else {
			return Literalf("LIMIT %d OFFSET %d", a[1], a[0])
		}
	default:
		return nonef("limit: %v", a)
	}
}

func limit(n int) Expression {
	if n < 0 {
		return empty("LIMIT")
	}
	return Literalf("LIMIT %d", n)
}

func Offset(n int) Expression {
	if n < 0 {
		return empty("OFFSET")
	}
	return Literalf("OFFSET %d", n)
}

type x byte

const X x = 'X'

func (x) Values(a ...interface{}) *QueryS {
	return Q3S2("(", ", ", ") VALUES (", ", ", ")", a...)
}

func (x) Set(a ...interface{}) *QueryS {
	return Q3S1("SET ", " = ", ", ", "", a...)
}

func (x) SelectDistinct(a ...Expression) *Query {
	return SelectDistinct().Append(a...)
}

func (x) Select(a ...Expression) *Query {
	return Select().Append(a...)
}

func (x) From(a ...Expression) *Query {
	return From().Append(a...)
}

func (x) OrderBy(a ...Expression) *Query {
	return &Query{'o', [...]string{"ORDER BY ", ", ", ""}, a}
}

func (x) GroupBy(a ...Expression) *Query {
	return GroupBy().Append(a...)
}

func (x) Union(a ...Expression) *Query {
	return Q3("", " UNION ", "", a...)
}

func (x) UnionAll(a ...Expression) *Query {
	return Q3("", " UNION ALL ", "", a...)
}
