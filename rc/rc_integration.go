package main

import (
	"log"
	"runtime"
	"strconv"
	"time"
)

import (
	riaken "github.com/riaken/riaken-core"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
	client := riaken.NewClient(addrs, 20, time.Second*2)
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
			bucket := session.GetBucket("rc-animals")
			object := bucket.Object("id-" + id)

			log.Printf("executing: %d", i)

			if _, err := object.Store([]byte("animal" + id)); err != nil {
				log.Print(err.Error())
			}

			if _, err := object.Fetch(); err != nil {
				log.Print(err.Error())
			}

			if _, err := object.Delete(); err != nil {
				log.Print(err.Error())
			}

			i++
		}()
	}

	<-done
}
