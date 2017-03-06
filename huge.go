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
	Starter  query.Starter
	Querier  Querier
	DealName func(string) string
	TimePrec int
}

func Open(driverName, dataSourceName string) (h Huge, err error) {
	h.Querier, err = sql.Open(driverName, dataSourceName)
	switch driverName {
	case "mysql":
		h.Starter = query.MySQLStarter
		h.TimePrec = 0
	case "postgres":
		h.Starter = query.PostgreSQLStarter
		h.TimePrec = 6
	case "sqlite":
		h.Starter = query.SQLiteStarter
		h.TimePrec = 9
	default:
		h.Starter = query.StandardStarter
		h.TimePrec = 6
	}
	return
}

func (h Huge) Now() time.Time {
	return h.LimitTime(time.Now())
}
func (h Huge) LimitTime(t time.Time) time.Time {
	return LimitTime(t, h.TimePrec)
}

func (h Huge) mustDB() *sql.DB {
	db, ok := h.Querier.(*sql.DB)
	if !ok {
		panic("huge: Querier is not sql.DB")
	}
	return db
}
func (h Huge) mustTx() *sql.Tx {
	tx, ok := h.Querier.(*sql.Tx)
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
	s, a, err := query.Expand(q, false, h.Starter, 1)
	log.Debugln(s, a)
	log.ErrDebug(err)
	return s, a, err
}
func (h Huge) Exec(q query.Expression) (sql.Result, error) {
	s, a, err := h.Expand(q)
	if err != nil {
		return nil, err
	}
	return h.Querier.Exec(s, a...)
}
func (h Huge) Prepare(q query.Expression) (*sql.Stmt, []interface{}, error) {
	s, a, err := h.Expand(q)
	if err != nil {
		return nil, nil, err
	}
	p, err := h.Querier.Prepare(s)
	return p, a, err
}
func (h Huge) Query(q query.Expression) *Rows {
	var rows *sql.Rows
	s, a, err := h.Expand(q)
	if err == nil {
		rows, err = h.Querier.Query(s, a...)
	}
	return &Rows{err, rows, h.DealName}
}
func (h Huge) Q(a ...query.Expression) *Rows {
	return h.Query(query.Q(a...))
}

func (h Huge) Begin() (_ Huge, err error) {
	h.Querier, err = h.mustDB().Begin()
	return h, err
}
func (h Huge) Commit() (err error) {
	return h.mustTx().Commit()
}
func (h Huge) Rollback() (err error) {
	return h.mustTx().Rollback()
}
