// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/cxr29/huge/query"
	"github.com/cxr29/log"
)

var (
	ErrNoRows = sql.ErrNoRows
	ErrTxDone = sql.ErrTxDone
)

type Querier interface {
	Exec(string, ...interface{}) (sql.Result, error)
	Prepare(string) (*sql.Stmt, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
}

type Huge struct {
	driverName, dataSourceName    string
	querier                       Querier
	TimePrecision, FirstParameter int
	ParameterFunc                 query.ParameterFunc
	QuotationFunc                 query.QuotationFunc
	TransformFunc                 query.TransformFunc
	ReturningFunc                 query.ReturningFunc
}

func Open(driverName, dataSourceName string) (h Huge, err error) {
	h.querier, err = sql.Open(driverName, dataSourceName)
	h.TimePrecision, h.FirstParameter = 6, 1
	switch driverName {
	case "mysql":
		h.TimePrecision = 0
		h.ParameterFunc = query.MySQLParameter
		h.QuotationFunc = query.MySQLQuotation
	case "postgres":
		h.ParameterFunc = query.PostgreSQLParameter
		h.QuotationFunc = query.PostgreSQLQuotation
		h.ReturningFunc = query.PostgreSQLReturning
	case "sqlite":
		h.ParameterFunc = query.SQLiteParameter
		h.QuotationFunc = query.SQLiteQuotation
	}
	return
}

func (h Huge) DB() *sql.DB {
	db, _ := h.querier.(*sql.DB)
	return db
}
func (h Huge) Tx() *sql.Tx {
	tx, _ := h.querier.(*sql.Tx)
	return tx
}
func (h Huge) mustDB() *sql.DB {
	db, ok := h.querier.(*sql.DB)
	if !ok {
		panic("huge: Querier is not sql.DB")
	}
	return db
}
func (h Huge) mustTx() *sql.Tx {
	tx, ok := h.querier.(*sql.Tx)
	if !ok {
		panic("huge: Querier is not sql.Tx")
	}
	return tx
}

func (h Huge) Close() error {
	return h.mustDB().Close()
}
func (h Huge) SetConnMaxLifetime(d time.Duration) {
	h.mustDB().SetConnMaxLifetime(d)
}
func (h Huge) SetMaxIdleConns(n int) {
	h.mustDB().SetMaxIdleConns(n)
}
func (h Huge) SetMaxOpenConns(n int) {
	h.mustDB().SetMaxOpenConns(n)
}
func (h Huge) Driver() driver.Driver {
	return h.mustDB().Driver()
}
func (h Huge) Stats() sql.DBStats {
	return h.mustDB().Stats()
}
func (h Huge) Ping() error {
	return h.mustDB().Ping()
}

func (h Huge) Expand(q query.Expression) (string, []interface{}, error) {
	s, a, err := query.Expand(q, false, h.FirstParameter, h.ParameterFunc, h.QuotationFunc)
	log.Debugln(s, a)
	log.ErrDebug(err)
	return s, a, err
}
func (h Huge) Exec(q query.Expression) (sql.Result, error) {
	s, a, err := h.Expand(q)
	if err != nil {
		return nil, err
	}
	return h.querier.Exec(s, a...)
}
func (h Huge) Prepare(q query.Expression) (*sql.Stmt, []interface{}, error) {
	s, a, err := h.Expand(q)
	if err != nil {
		return nil, nil, err
	}
	p, err := h.querier.Prepare(s)
	return p, a, err
}
func (h Huge) Query(q query.Expression) *Rows {
	var rows *sql.Rows
	s, a, err := h.Expand(q)
	if err == nil {
		rows, err = h.querier.Query(s, a...)
	}
	return &Rows{err: err, rows: rows, TransformFunc: h.TransformFunc}
}
func (h Huge) Q(a ...query.Expression) *Rows {
	return h.Query(query.Q(a...))
}

func (h Huge) Begin() (_ Huge, err error) {
	h.querier, err = h.mustDB().Begin()
	return h, err
}
func (h Huge) Commit() (err error) {
	return h.mustTx().Commit()
}
func (h Huge) Rollback() (err error) {
	return h.mustTx().Rollback()
}
