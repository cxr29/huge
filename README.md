Go huge CRUD library and SQL builder
===
Reinventing the wheel, reinventing the best wheel.

### Features
* CRUD/Load/Upsert/Convert/RUD by Primary Key
* Auto Increment/Auto Now/Auto Now Add
* Encoding GOB/JSON/XML
* Collapse SQL NULL&Go Zero Value
* Version
* Inline/Inline Static
* Primary Key/Foreign Key/One to One/One to Many/Many to One/Many to Many
* Scan One/All to Struct/Slice/Map/Array
* Scan interface{} Slice/Map with Type
* Time Precision/Unix Seconds/Unix Milliseconds/Integer Date
* Exclude Columns/Transform Column Name
* Building SQL Programmatically/SQL Debug Log

### Usage
```go
package main

import (
  "fmt"
  "time"

  "github.com/cxr29/huge"
  "github.com/cxr29/huge/query"
  "github.com/cxr29/log"
)

type Node struct {
  Id       int
  Name     string
  Code     string                 `huge:",collapse"`
  Data     map[string]interface{} `huge:",json"`
  Parent   *Node                  `huge:",foreign_key"`
  Children []*Node                `huge:",one_to_many"`
  Siblings map[int]*Node          `huge:",many_to_many"`
  Version  int                    `huge:",version"`
  Created  int64                  `huge:",auto_now_add"`
  Updated  time.Time              `huge:",auto_now"`
}

func main() {
  n := &Node{ /* ... */ }

  h, err := huge.Open("driverName", "dataSourceName")
  log.ErrPanic(err)

  // CRUD row
  r, err := h.Create(n)
  log.ErrPanic(err)
  fmt.Println(r.(bool), n.Id, n.Name, n.Version, n.Created, n.Updated)

  n.Name = "..."
  r, err = h.Update(n, "Name")
  log.ErrPanic(err)
  fmt.Println(r.(bool), n.Id, n.Name, n.Version, n.Created, n.Updated)

  a := &Node{Id: n.Id, Version: -1} // force version
  r, err = h.Read(a)
  log.ErrPanic(err)
  fmt.Println(r.(bool), a.Id, a.Name, a.Version, a.Created, a.Updated)

  r, err = h.Delete(a)
  log.ErrPanic(err)
  fmt.Println(r.(bool))

  // CRUD rows
  s := []*Node{ /* ... */ }
  r, err = h.Create(s)
  log.ErrPanic(err)
  fmt.Println(r.(int))

  m := map[string]*Node{ /* ... */ }
  r, err = h.Read(m, huge.Exclude, "Data") // exlcude columns
  log.ErrPanic(err)
  fmt.Println(r.(map[string]struct{}))

  r, err = h.Update(m)
  log.ErrPanic(err)
  fmt.Println(r.(map[string]struct{}))

  r, err = h.Delete(s)
  log.ErrPanic(err)
  fmt.Println(r.(map[int]struct{}))

  // RUD by Primary Keys
  keys := map[int]struct{}{ /* ... */ }
  r, err = h.ReadBy(keys, Node{})
  log.ErrPanic(err)
  fmt.Println(r.(map[int]*Node))

  r, err = h.UpdateBy([]int{7, 29}, &Node{Name: "foobar", Version: 2}, "Name") // only version = 2
  log.ErrPanic(err)
  fmt.Println(r.(int))

  r, err = h.DeleteBy(1, Node{}) // only one primary key
  log.ErrPanic(err)
  fmt.Println(r.(int))

  // Build SQL and Scan
  var all []*Node
  log.ErrPanic(h.Q(
    query.Select(),
    query.From("Node"),
    query.Where(
      query.Contains("Name", "foo"),
      query.Eq("Version", 1),
    ),
    query.OrderBy("-Code", "+Id"),
  ).All(&all))
  fmt.Println(all)

  version := query.IQ("Version")
  set := query.X.Set()
  set.Add("Name", "foo")
  set.Append(version, version.Inc())
  result, err := h.Exec(query.Q(
    query.Update("Node"),
    set,
    query.Where(query.Eq("Id", 10)),
  ))
  log.ErrPanic(err)
  affected, err := result.RowsAffected()
  log.ErrPanic(err)
  fmt.Println(affected)

  // ...
}
```

##### I hate writing documentation but [RTFSC](https://godoc.org/github.com/cxr29/huge).
##### I hate writing test cases but I have tested it. I did my best.
