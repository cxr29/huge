// Copyright (c) 2017 CHEN Xianren. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package query

import (
	"bytes"
)

type Creater interface {
	CreateTable(string, []string, bool, bool) string
}

func CreateTable(tableName string, columns []string, temporary, ifNotExists bool) string {
	b := CreateTableBuffer(tableName, columns, temporary, ifNotExists)
	b.WriteString(";\n")
	return b.String()
}

func CreateTableBuffer(tableName string, columns []string, temporary, ifNotExists bool) *bytes.Buffer {
	var b bytes.Buffer
	b.WriteString("CREATE")
	if temporary {
		b.WriteString(" TEMPORARY")
	}
	b.WriteString(" TABLE")
	if ifNotExists {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteByte(' ')
	b.WriteString(tableName)
	b.WriteString(" (\n")
	for i, c := range columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteByte('\t')
		b.WriteString(c)
	}
	b.WriteString("\n)")
	return &b
}
