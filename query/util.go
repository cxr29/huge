// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"sort"
	"strings"
)

func escape(s, e string, c byte) string {
	for i := 0; i < len(s); i++ {
		if strings.IndexByte(e, s[i]) >= 0 {
			b := make([]byte, i, len(s)+2+len(e))
			copy(b, s[:i])
			b = append(b, c, s[i])
			for i++; i < len(s); i++ {
				if strings.IndexByte(e, s[i]) >= 0 {
					b = append(b, c)
				}
				b = append(b, s[i])
			}
			return string(b)
		}
	}
	return s
}

func EscapeLike(s string) string {
	return escape(s, `\_%`, '\\')
}

func EscapeRegexp(s string) string {
	return escape(s, `\.+*?()|[]{}^$`, '\\')
}

func Quote(s string, c byte) string {
	b := make([]byte, 0, len(s)+2+2)
	b = append(b, c)
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			b = append(b, c)
		}
		b = append(b, s[i])
	}
	b = append(b, c)
	return string(b)
}

func Quoted(s string, c byte) string {
	n := len(s) - 1
	if n < 1 || s[0] != s[n] {
		return ""
	} else if s[0] == c {
		for i := 1; i < n; i++ {
			if s[i] == c {
				if j := i + 1; j < n && s[j] == c {
					i = j
				} else {
					return ""
				}
			}
		}
		return s
	}
	b := make([]byte, 0, len(s)+2)
	b = append(b, c)
	for i := 1; i < n; i++ {
		switch s[i] {
		case s[0]:
			if j := i + 1; j < n && s[j] == s[0] {
				b = append(b, s[0])
				i = j
			} else {
				return ""
			}
		case c:
			b = append(b, c, c)
		default:
			b = append(b, s[i])
		}
	}
	b = append(b, c)
	return string(b)
}

const maxLen = 63

func notAllow(r rune) bool {
	switch r {
	case '\u0000', '"', '\'', '\\', '`', '\uFFFD':
		return true
	default:
		return false
	}
}

func isDigit(r rune) bool {
	return '0' <= r && r <= '9'
}

func isUpper(r rune) bool {
	return 'A' <= r && r <= 'Z'
}

func isLower(r rune) bool {
	return 'a' <= r && r <= 'z'
}

func isLetter(r rune) bool {
	return isLower(r) || isUpper(r)
}

func init() {
	sort.Strings(MySQLKeywords)
	sort.Strings(PostgreSQLKeywords)
	sort.Strings(SQLiteKeywords)
}

func IsKeyword(a []string, s string) bool {
	s = strings.ToUpper(s)
	i := sort.SearchStrings(a, s)
	return i >= 0 && i < len(a) && a[i] == s
}
