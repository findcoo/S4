package main

import (
	"io"
	"log"
)

func GetReader(r *io.Reader) <-chan string {
	buff := make([]byte, 1024)
	in := make(chan<- string, 1)

	go func() {
		for {
			n, err := r.Read(buff[:])
			if err != nil {
				log.Panic(err)
			}
			defer close(in)

			in <- string(buff[0:n])
		}
	}()

	return in
}
