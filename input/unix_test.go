package input

import (
	"bytes"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

func TestRead(t *testing.T) {
	ready := unixStreamServer()
	<-ready
	us := OpenUnixSocket("./test.sock")
	out := us.Subscribe()

	for {
		data := <-out

		if ok := bytes.Equal(data, []byte("world")); ok {
			log.Print("end subscribe")
			return
		}

		log.Printf("subscribe data: %s ", string(data))
	}
}

func echo(c net.Conn) bool {
	var err error

	for i := 0; i <= 5; i++ {
		time.Sleep(time.Millisecond * 200)

		if i == 5 {
			_, err = c.Write([]byte("world"))
			_ = c.Close()
			return true
		}
		_, err = c.Write([]byte("hello this byte stream test!"))
		if err != nil {
			log.Fatal(err)
		}
	}

	return false
}

func unixStreamServer() <-chan struct{} {
	_ = os.Remove("./test.sock")
	ready := make(chan struct{}, 1)
	sock, err := net.Listen("unix", "./test.sock")
	if err != nil {
		log.Fatal(err)
	}

	ready <- struct{}{}

	go func() {
		for {
			fd, err := sock.Accept()
			if err != nil {
				log.Fatal(err)
			}

			if ok := echo(fd); ok {
				_ = sock.Close()
				return
			}
		}
	}()

	return ready
}
