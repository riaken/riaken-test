package main

import (
	"log"
	"runtime"
	"strconv"
)

import (
	riaken "github.com/riaken/riaken-core"
)

const LIMIT int = 1000

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
	client := riaken.NewClient(addrs, 10)
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
				bucket := session.GetBucket("rc-animals")
				object := bucket.Object("id-" + id)

				if _, err := object.Store([]byte("animal" + id)); err != nil {
					log.Print(err.Error())
					return
				}

				if _, err := object.Fetch(); err != nil {
					log.Print(err.Error())
					return
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
