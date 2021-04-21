// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

type SQLite struct{}

var (
	SQLiteStarter         = SQLite{}
	_             Starter = SQLiteStarter
)

func (SQLite) Dialect() string {
	return "sqlite3"
}

func (SQLite) Parameter(n bool, i int) string {
	if n {
		return "?" + strconv.Itoa(i)
	} else {
		return "?"
	}
}

func (SQLite) Quote(s string) string {
	if len(s) == 0 || len(s) > maxLen {
		return ""
	}
	q := false
	for _, r := range s {
		switch {
		case notAllow(r):
			return ""
		case isDigit(r) || isLower(r) || r == '_':
		default:
			q = true
		}
	}
	if !q {
		r, _ := utf8.DecodeRuneInString(s)
		q = !isLetter(r) && r != '_'
	}
	if !q {
		q = IsKeyword(SQLiteKeywords, s)
	}
	if q {
		return `"` + s + `"`
	} else {
		return s
	}
}

func (sqlite SQLite) Quoted(s string) string {
	if s[0] == '\'' {
		return s
	} else {
		return sqlite.Quote(s[1 : len(s)-1])
	}
}

func (SQLite) Returning(byte, string) string {
	return ""
}

func (SQLite) Mapping(_, goType string, maxSize, option int) (_ string, optionValue string) {
	switch option {
	case OptionAutoIncrement:
		optionValue = "AUTOINCREMENT"
	case OptionAutoNow, OptionAutoNowAdd:
		if goType == "time" {
			optionValue = "DEFAULT CURRENT_TIMESTAMP"
		} else {
			optionValue = "DEFAULT 0"
		}
	case OptionVersion:
		optionValue = "DEFAULT 1"
	}
	switch goType {
	case "bool":
		return "BOOLEAN", "FALSE"
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		if option == OptionZeroValue {
			optionValue = "0"
		}
		return "INTEGER", optionValue
	case "float32", "float64":
		return "REAL", "0"
	case "time":
		if option == OptionZeroValue {
			optionValue = "'1970-01-01T00:00:00Z'"
		}
		return "DATETIME", optionValue
	case "bytes", "gob":
		return "BLOB", ""
	case "string": // interface, json, xml
		optionValue = "''"
		fallthrough
	default:
		if maxSize == 0 {
			return "VARCHAR(255)", optionValue
		} else if maxSize > 0 && maxSize <= 255 {
			return fmt.Sprintf("VARCHAR(%d)", maxSize), optionValue
		} else {
			return "TEXT", optionValue
		}
	}
}

var SQLiteKeywords = strings.Split(strings.ToUpper(`ABORT
ACTION
ADD
AFTER
ALL
ALTER
ANALYZE
AND
AS
ASC
ATTACH
AUTOINCREMENT
BEFORE
BEGIN
BETWEEN
BY
CASCADE
CASE
CAST
CHECK
COLLATE
COLUMN
COMMIT
CONFLICT
CONSTRAINT
CREATE
CROSS
CURRENT_DATE
CURRENT_TIME
CURRENT_TIMESTAMP
DATABASE
DEFAULT
DEFERRABLE
DEFERRED
DELETE
DESC
DETACH
DISTINCT
DROP
EACH
ELSE
END
ESCAPE
EXCEPT
EXCLUSIVE
EXISTS
EXPLAIN
FAIL
FOR
FOREIGN
FROM
FULL
GLOB
GROUP
HAVING
IF
IGNORE
IMMEDIATE
IN
INDEX
INDEXED
INITIALLY
INNER
INSERT
INSTEAD
INTERSECT
INTO
IS
ISNULL
JOIN
KEY
LEFT
LIKE
LIMIT
MATCH
NATURAL
NO
NOT
NOTNULL
NULL
OF
OFFSET
ON
OR
ORDER
OUTER
PLAN
PRAGMA
PRIMARY
QUERY
RAISE
RECURSIVE
REFERENCES
REGEXP
REINDEX
RELEASE
RENAME
REPLACE
RESTRICT
RIGHT
ROLLBACK
ROW
SAVEPOINT
SELECT
SET
TABLE
TEMP
TEMPORARY
THEN
TO
TRANSACTION
TRIGGER
UNION
UNIQUE
UPDATE
USING
VACUUM
VALUES
VIEW
VIRTUAL
WHEN
WHERE
WITH
WITHOUT`), "\n")
