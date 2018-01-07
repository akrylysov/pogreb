# Pogreb

Pogreb is an embedded key/value store for read-heavy workloads written in Go.

## Key characteristics

- 100% Go.
- High read performance.
- Can store larger-than-memory data sets.
- Low memory usage.
- All DB methods are safe for concurrent use by multiple goroutines.

## Installation

```sh
$ go get -u github.com/akrylysov/pogreb
```

## Usage

### Opening a database

To open or create a new database, use the `pogreb.Open()` function:

```go
package main

import (
	"log"

	"github.com/akrylysov/pogreb"
)

func main() {
    db, err := pogreb.Open("pogreb.test", nil)
    if err != nil {
        log.Fatal(err)
        return
    }	
    defer db.Close()
}
```

### Writing to a database

Use the `DB.Put()` function to insert a new key/value pair:

```go
err := b.Put([]byte("testKey"), []byte("testValue"))
if err != nil {
	log.Fatal(err)
}
```

### Reading from a database

Use the `DB.Get()` function to retrieve the inserted value:

```go
val, err := b.Get([]byte("testKey"))
if err != nil {
	log.Fatal(err)
}
log.Info(val)
```

### Iterating over items

Use the `DB.Items()` function which returns a new instance of `ItemIterator`:

```go
it := db.Items()
for {
    key, val, err := it.Next()
    if err != nil {
        if err != pogreb.ErrIterationDone {
            log.Fatal(err)
        }
        break
    }
    log.Info(key, val)
}
```
