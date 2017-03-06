// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

type join struct {
	e Expression
}

func (a *join) Expand(s Starter, i int) (string, []interface{}, error) {
	return Expand(a.e, false, s, i)
}

func (a *join) Join(operator string, b interface{}) *joinC {
	return newJoin(a, operator, a.e, mustSE(b))
}
func (a *join) InnerJoin(b interface{}) *joinC {
	return a.Join("INNER", b)
}
func (a *join) LeftJoin(b interface{}) *joinC {
	return a.Join("LEFT", b)
}
func (a *join) RightJoin(b interface{}) *joinC {
	return a.Join("RIGHT", b)
}

func (a *join) NaturalJoin(operator string, b interface{}) *join {
	return newNaturalJoin(a, operator, a.e, mustSE(b))
}
func (a *join) NaturalLeftJoin(b interface{}) *join {
	return a.NaturalJoin("LEFT", b)
}
func (a *join) NaturalRightJoin(b interface{}) *join {
	return a.NaturalJoin("RIGHT", b)
}

type joinC struct {
	*join
}

func (c *joinC) On(a ...Condition) *join {
	j := c.join
	c.join = nil
	if len(a) > 0 {
		j.e = q2("ON", j.e, And(a...))
	}
	return j
}
func (c *joinC) Using(a ...string) *join {
	j := c.join
	c.join = nil
	if len(a) > 0 {
		j.e = q2("USING", j.e, Q3("(", ", ", ")").Add(a...))
	}
	return j
}

func mustSE(i interface{}) Expression {
	switch x := i.(type) {
	case Expression:
		return x
	case string:
		return Identifier(x)
	}
	panic("neither string nor expression")
}

func newJoin(j *join, operator string, a, b Expression) *joinC {
	if len(operator) > 0 {
		operator = operator + " JOIN"
	} else {
		operator = "JOIN"
	}
	if j == nil {
		j = new(join)
	}
	j.e = q2(operator, a, b)
	return &joinC{j}
}
func Join(operator string, a, b interface{}) *joinC {
	return newJoin(nil, operator, mustSE(a), mustSE(b))
}
func InnerJoin(a, b interface{}) *joinC {
	return Join("INNER", a, b)
}
func LeftJoin(a, b interface{}) *joinC {
	return Join("LEFT", a, b)
}
func RightJoin(a, b interface{}) *joinC {
	return Join("RIGHT", a, b)
}

func newNaturalJoin(j *join, operator string, a, b Expression) *join {
	if len(operator) > 0 {
		operator = "NATURAL " + operator + " JOIN"
	} else {
		operator = "NATURAL JOIN"
	}
	if j == nil {
		j = new(join)
	}
	j.e = q2(operator, a, b)
	return j
}
func NaturalJoin(operator string, a, b interface{}) *join {
	return newNaturalJoin(nil, operator, mustSE(a), mustSE(b))
}
func NaturalLeftJoin(a, b interface{}) *join {
	return NaturalJoin("LEFT", a, b)
}
func NaturalRightJoin(a, b interface{}) *join {
	return NaturalJoin("RIGHT", a, b)
}
