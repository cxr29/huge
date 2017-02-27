// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package huge

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

var precisions = [...]int{1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8}

func limitTimePrecision(p int, t time.Time) time.Time {
	if p == 0 {
		return time.Unix(t.Unix(), 0)
	} else if 1 <= p && p <= 8 {
		p = precisions[8-p]
		return time.Unix(t.Unix(), int64(t.Nanosecond()/p*p))
	}
	return t
}

func (c *Column) convertTime(p int, t time.Time) interface{} {
	f := c.last()
	if x := f.Type(); x == typeTime {
		t = limitTimePrecision(p, t)
		if f.Is(oPointer) {
			return &t
		} else {
			return t
		}
	} else {
		switch x.Kind() {
		case reflect.Int:
			if i := int(t.Unix()); f.Is(oPointer) {
				return &i
			} else {
				return i
			}
		case reflect.Int32:
			if i := int32(t.Unix()); f.Is(oPointer) {
				return &i
			} else {
				return i
			}
		case reflect.Uint:
			if i := uint(t.Unix()); f.Is(oPointer) {
				return &i
			} else {
				return i
			}
		case reflect.Uint32:
			if i := uint32(t.Unix()); f.Is(oPointer) {
				return &i
			} else {
				return i
			}
		case reflect.Int64:
			if i := ToUnix(t); f.Is(oPointer) {
				return &i
			} else {
				return i
			}
		case reflect.Uint64:
			if i := uint64(ToUnix(t)); f.Is(oPointer) {
				return &i
			} else {
				return i
			}
		}
	}
	return nil
}

func (c *Column) setTime(v reflect.Value, p int, t time.Time) bool {
	if v, ok := c.field(v); ok {
		var i interface{}
		f := c.last()
		x := f.Type()
		if x == typeTime {
			i = limitTimePrecision(p, t)
		} else {
			switch x.Kind() {
			case reflect.Int:
				i = int(t.Unix())
			case reflect.Int32:
				i = int32(t.Unix())
			case reflect.Int64:
				i = ToUnix(t)
			case reflect.Uint:
				i = uint(t.Unix())
			case reflect.Uint32:
				i = uint32(t.Unix())
			case reflect.Uint64:
				i = uint64(ToUnix(t))
			}
		}
		if i != nil {
			if f.Is(oPointer) {
				if v.IsNil() {
					if v.CanSet() {
						v.Set(reflect.New(x))
					} else {
						return false
					}
				}
				v = v.Elem()
			}
			if v.CanSet() {
				v.Set(reflect.ValueOf(i))
				return true
			}
		}
	}
	return false
}

const (
	Millisecond = 1
	Second      = 1e3 * Millisecond
	Minute      = 60 * Second
	Hour        = 60 * Minute
	Day         = 24 * Hour
	Week        = 7 * Day
)

func NowUnix() int64 {
	return ToUnix(time.Now())
}

func FromUnix(ms int64) time.Time {
	return time.Unix(ms/1e3, ms%1e3*1e6)
}

func ToUnix(t time.Time) int64 {
	return t.Unix()*1e3 + int64(t.Nanosecond()/1e6)
}

func isYear(y int) bool {
	return 1 <= y && y <= 9999
}

func isLeap(y int) bool {
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

func isMonth(m int) bool {
	return 1 <= m && m <= 12
}

var monthDays = [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func maxMonthDay(m int, leap bool) int {
	d := monthDays[m-1]
	if m == 2 && leap {
		d++
	}
	return d
}

func isDate(y, m, d int) bool {
	return isYear(y) && isMonth(m) && 1 <= d && d <= maxMonthDay(m, isLeap(y))
}

func NowDate() int {
	return ToDate(time.Now())
}

func splitDate(n int) (y, m, d int, x bool) {
	x = n < 0
	if x {
		n = -n
	}
	y = n / 1e4
	m = n / 1e2 % 1e2
	d = n % 1e2
	return
}

func IsDate(n int) bool {
	y, m, d, _ := splitDate(n)
	return isDate(y, m, d)
}

func FromDate(n int) (t time.Time, ok bool) {
	y, m, d, x := splitDate(n)
	ok = isDate(y, m, d)
	if ok {
		if x {
			y = -y
		}
		t = time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.Local)
	}
	return
}

func ToDate(t time.Time) int {
	t = t.Local()
	y := t.Year()
	x := y < 0
	if x {
		y = -y
	}
	return newDate(y, int(t.Month()), t.Day(), x)
}

func newDate(y, m, d int, x bool) (n int) {
	if isDate(y, m, d) {
		n = y*1e4 + m*1e2 + d
		if x {
			n = -n
		}
	}
	return
}

var dateRegexp = regexp.MustCompile(`^(\d{1,4})[-/](\d{1,2})[-/](\d{1,2})$`)

func ParseDate(s string) int {
	if len(s) == 0 {
		return 0
	}
	x := s[0] == '-'
	if x {
		s = s[1:]
	}
	a := dateRegexp.FindStringSubmatch(s)
	if a == nil {
		return 0
	}
	y, _ := strconv.Atoi(a[1])
	m, _ := strconv.Atoi(a[2])
	d, _ := strconv.Atoi(a[3])
	return newDate(y, m, d, x)
}

func FormatDate(n int) (s string) {
	y, m, d, x := splitDate(n)
	if ok := isDate(y, m, d); ok {
		if x {
			y = -y
		}
		s = fmt.Sprintf("%04d-%02d-%02d", y, m, d)
	}
	return
}
