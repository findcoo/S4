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
	out := us.Read()

	for {
		data := <-out

		if ok := bytes.Equal(data, []byte("world")); ok {
			log.Print("end subscribe")
			return
		}

		log.Printf("subscribe data: %s ", string(data))
	}
}

func echo(c net.Conn) {
	var err error

	for i := 0; i <= 5; i++ {
		time.Sleep(time.Millisecond * 200)

		if i == 5 {
			_, err = c.Write([]byte("world"))
			_ = c.Close()
			return
		}
		_, err = c.Write([]byte("hello!"))
		if err != nil {
			log.Fatal(err)
		}
	}
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

			go echo(fd)
		}
	}()

	return ready
}
