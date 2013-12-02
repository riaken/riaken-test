package main

import (
	"log"
	"runtime"
	"strconv"
)

import (
	riaken "github.com/riaken/riaken-struct"
)

const LIMIT int = 1000

type Animal struct {
	Name string `json:"name"`
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	marshaller := riaken.NewStructMarshal("json", riaken.JsonMarshaller, riaken.JsonUnmarshaller)
	addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
	client := riaken.NewClient(addrs, 10, marshaller)
	//client.Debug(true)
	defer client.Close()
	client.Dial()

	done := make(chan bool)

	i := 0
	for {
		sem := make(chan bool, LIMIT)
		for t := 0; t < LIMIT; t++ {
			go func(i int) {
				defer func() {
					sem <- true
				}()
				session := client.Session()
				defer session.Release()

				id := strconv.FormatInt(int64(i), 10)
				bucket := session.GetBucket("rs-animals")
				object := bucket.Object("id-" + id)
				animal := &Animal{
					Name: "animal" + id,
				}

				if _, err := object.Store(animal); err != nil {
					log.Print(err.Error())
					return
				}

				var check Animal
				if _, err := object.Fetch(&check); err != nil {
					log.Print(err.Error())
					return
				}

				if check.Name != animal.Name {
					log.Print("name mismatch!")
				}

				if _, err := object.Delete(); err != nil {
					log.Print(err.Error())
					return
				}

				if i%100 == 0 {
					log.Printf("executing: %d", i)
				}

			}(i)
			i++
		}
		for t := 0; t < LIMIT; t++ {
			<-sem
		}
	}

	<-done
}
