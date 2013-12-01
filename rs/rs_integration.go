package main

import (
	"log"
	"runtime"
	"strconv"
	"time"
)

import (
	riaken "github.com/riaken/riaken-struct"
)

type Animal struct {
	Name string `json:"name"`
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	marshaller := riaken.NewStructMarshal("json", riaken.JsonMarshaller, riaken.JsonUnmarshaller)
	addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
	client := riaken.NewClient(addrs, 20, time.Second*2, marshaller)
	defer client.Close()
	client.Dial()

	done := make(chan bool)

	i := 0
	for {
		go func() {
			session, err := client.Session()
			if err != nil {
				log.Print(err.Error())
				return
			}
			defer session.Close()

			id := strconv.FormatInt(int64(i), 10)
			bucket := session.GetBucket("rs-animals")
			object := bucket.Object("id-" + id)
			animal := &Animal{
				Name: "animal" + id,
			}

			log.Printf("executing: %d", i)

			if _, err := object.Store(animal); err != nil {
				log.Print(err.Error())
			}

			var check Animal
			if _, err := object.Fetch(&check); err != nil {
				log.Print(err.Error())
			}

			if check.Name != animal.Name {
				log.Print("name mismatch!")
			}

			if _, err := object.Delete(); err != nil {
				log.Print(err.Error())
			}

			i++
		}()
	}

	<-done
}
