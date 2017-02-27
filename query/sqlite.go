// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

func SQLiteParameter(i int, n bool) string {
	if i < 1 {
		return ""
	} else if n {
		return "?" + strconv.Itoa(i)
	}
	return "?"
}

func SQLiteQuotation(s string) string {
	if s[0] == '\'' {
		return s
	}
	s = s[1 : len(s)-1]
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
		s = `"` + s + `"`
	}
	return s
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
