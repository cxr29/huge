// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"bytes"
	"strconv"
)

type Operand struct {
	e Expression
}

func (o Operand) Expand(n int, p ParameterFunc, q QuotationFunc) (string, []interface{}, error) {
	return Expand(o.e, false, n, p, q)
}

func E2O(e Expression) Operand {
	switch o := e.(type) {
	case Operand:
		return o
	case *Operand:
		return *o
	}
	return Operand{e}
}

func O(format string, a ...interface{}) Operand {
	return Operand{E(format, a...)}
}

func IQ(a ...string) Operand {
	if len(a) == 1 {
		return Operand{Identifier(a[0])}
	}
	return Operand{Qualifier(a)}
}

func (o Operand) IsNull() Condition {
	return C("? IS NULL", o)
}

func (o Operand) Eq(i interface{}) Condition {
	if i == nil {
		return o.IsNull()
	}
	return C("? = ?", o, i)
}

func (o Operand) Lt(i interface{}) Condition {
	return C("? < ?", o, i)
}

func (o Operand) Le(i interface{}) Condition {
	return C("? <= ?", o, i)
}

func (o Operand) Gt(i interface{}) Condition {
	return C("? > ?", o, i)
}

func (o Operand) Ge(i interface{}) Condition {
	return C("? >= ?", o, i)
}

func (o Operand) InInts(a ...int) Condition {
	if len(a) == 0 {
		return E2C(nonef("empty in: %v", o))
	}
	var b bytes.Buffer
	b.WriteString("? IN (")
	for k, v := range a {
		if k > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Itoa(v))
	}
	b.WriteByte(')')
	return C(b.String(), o)
}

func (o Operand) InStrings(a ...string) Condition {
	if len(a) == 0 {
		return E2C(nonef("empty in: %v", o))
	}
	var b bytes.Buffer
	b.WriteString("? IN (")
	for k, v := range a {
		if k > 0 {
			b.WriteString(", ")
		}
		b.WriteString(Quote(v, '\''))
	}
	b.WriteByte(')')
	return C(b.String(), o)
}

func (o Operand) In(a ...interface{}) Condition {
	if len(a) == 0 {
		return E2C(nonef("empty in: %v", o))
	}
	var b bytes.Buffer
	b.WriteString("? IN (")
	for i := 0; i < len(a); i++ {
		if a[i] == nil {
			return E2C(nonef("null in: %v", o))
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('?')
	}
	b.WriteByte(')')
	d := make([]interface{}, 1+len(a))
	d[0] = o
	copy(d[1:], a)
	return C(b.String(), d...)
}

func (o Operand) Between(i, j interface{}) Condition {
	return C("? BETWEEN ? AND ?", o, i, j)
}

func (o Operand) Like(s string) Condition {
	return C("? LIKE ?", o, s)
}

func (o Operand) Contains(s string) Condition {
	return o.Like("%" + EscapeLike(s) + "%")
}

func (o Operand) HasPrefix(s string) Condition {
	return o.Like(EscapeLike(s) + "%")
}

func (o Operand) HasSuffix(s string) Condition {
	return o.Like("%" + EscapeLike(s))
}

func (o Operand) Asc() Expression {
	return E("? ASC", o)
}

func (o Operand) Desc() Expression {
	return E("? DESC", o)
}

func (o Operand) Inc() Expression {
	return E("? + 1", o)
}

func (o Operand) Dec() Expression {
	return E("? - 1", o)
}

func (o Operand) Avg() Expression {
	return E("AVG(?)", o)
}

func (o Operand) Count() Expression {
	return E("COUNT(?)", o)
}

func (o Operand) Max() Expression {
	return E("MAX(?)", o)
}

func (o Operand) Min() Expression {
	return E("MIN(?)", o)
}

func (o Operand) Sum() Expression {
	return E("SUM(?)", o)
}

func (o Operand) As(s string) Expression {
	return E("? AS ?", o, Identifier(s))
}

func (a Operand) InnerJoin(b Expression) Expression {
	return E("? INNER JOIN ?", a, b)
}

func (a Operand) LeftJoin(b Expression) Expression {
	return E("? LEFT JOIN ?", a, b)
}

func (a Operand) RightJoin(b Expression) Expression {
	return E("? RIGHT JOIN ?", a, b)
}

func (a Operand) FullJoin(b Expression) Expression {
	return E("? FULL JOIN ?", a, b)
}

func (a Operand) Union(b Expression) Expression {
	return E("? UNION ?", a, b)
}

func (a Operand) UnionAll(b Expression) Expression {
	return E("? UNION ALL ?", a, b)
}

func IsNull(c string) Condition {
	return IQ(c).IsNull()
}

func Eq(c string, i interface{}) Condition {
	return IQ(c).Eq(i)
}

func Lt(c string, i interface{}) Condition {
	return IQ(c).Lt(i)
}

func Le(c string, i interface{}) Condition {
	return IQ(c).Le(i)
}

func Gt(c string, i interface{}) Condition {
	return IQ(c).Gt(i)
}

func Ge(c string, i interface{}) Condition {
	return IQ(c).Ge(i)
}

func InInts(c string, a ...int) Condition {
	return IQ(c).InInts(a...)
}

func InStrings(c string, a ...string) Condition {
	return IQ(c).InStrings(a...)
}

func In(c string, a ...interface{}) Condition {
	return IQ(c).In(a...)
}

func Between(c string, i, j interface{}) Condition {
	return IQ(c).Between(i, j)
}

func Like(c, s string) Condition {
	return IQ(c).Like(s)
}

func Contains(c, s string) Condition {
	return IQ(c).Contains(s)
}

func HasPrefix(c, s string) Condition {
	return IQ(c).HasPrefix(s)
}

func HasSuffix(c, s string) Condition {
	return IQ(c).HasSuffix(s)
}

func Asc(c string) Expression {
	return IQ(c).Asc()
}

func Desc(c string) Expression {
	return IQ(c).Desc()
}

func Inc(c string) Expression {
	return IQ(c).Inc()
}

func Dec(c string) Expression {
	return IQ(c).Dec()
}

func Avg(c string) Expression {
	return IQ(c).Avg()
}

func Count(c string) Expression {
	return IQ(c).Count()
}

func Max(c string) Expression {
	return IQ(c).Max()
}

func Min(c string) Expression {
	return IQ(c).Min()
}

func Sum(c string) Expression {
	return IQ(c).Sum()
}

func As(c, s string) Expression {
	return IQ(c).As(s)
}

func InnerJoin(a, b string) Expression {
	return IQ(a).InnerJoin(Identifier(b))
}

func LeftJoin(a, b string) Expression {
	return IQ(a).LeftJoin(Identifier(b))
}

func RightJoin(a, b string) Expression {
	return IQ(a).RightJoin(Identifier(b))
}

func FullJoin(a, b string) Expression {
	return IQ(a).FullJoin(Identifier(b))
}

func (x) InnerJoin(a, b Expression) Expression {
	return E2O(a).InnerJoin(b)
}

func (x) LeftJoin(a, b Expression) Expression {
	return E2O(a).LeftJoin(b)
}

func (x) RightJoin(a, b Expression) Expression {
	return E2O(a).RightJoin(b)
}

func (x) FullJoin(a, b Expression) Expression {
	return E2O(a).FullJoin(b)
}

func (x) Union(a, b Expression) Expression {
	return E2O(a).Union(b)
}

func (x) UnionAll(a, b Expression) Expression {
	return E2O(a).UnionAll(b)
}
