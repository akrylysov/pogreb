package pogreb_test

import (
	"log"

	"github.com/akrylysov/pogreb"
)

func Example() {
	db, err := pogreb.Open[string, string]("pogreb.test", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	// Insert a new key-value pair.
	if err := db.Put("testKey", "testValue"); err != nil {
		log.Fatal(err)
	}

	// Retrieve the inserted value.
	val, err := db.Get("testKey")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", val)

	// Iterate over items.
	it := db.Items()
	for {
		key, val, err := it.Next()
		if err == pogreb.ErrIterationDone {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%s %s", key, val)
	}
}
