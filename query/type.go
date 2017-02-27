// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

const meta = "\"$'?`"

var marks = [...]string{
	"double quote",
	"dollar sign",
	"single quote",
	"question mark",
	"back quote",
}

type ParameterFunc func(int, bool) string
type QuotationFunc func(string) string
type TransformFunc func(string) string
type ReturningFunc func(byte, Operand) Expression

type Expression interface {
	Expand(int, ParameterFunc, QuotationFunc) (string, []interface{}, error)
}

type Condition interface {
	And(...Condition) Condition
	Expression
	Not() Condition
	Or(...Condition) Condition
}

type x byte

const X x = 'X'
