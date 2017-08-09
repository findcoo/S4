package test

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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

// UnixTestServer unix socket을 테스트하기 위한 테스트 서버
func UnixTestServer() <-chan struct{} {
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

// MockUnixEchoServer unix socket mocking server
func MockUnixEchoServer(timeout time.Duration) {
	sock, err := net.Listen("unix", "./mock.sock")
	if err != nil {
		log.Fatal(err)
	}
	var count uint32
	ticker := time.NewTicker(time.Second * 1)
	buff := make([]byte, 4)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func() {
		deadSign := <-sig
		sock.Close()
		log.Fatal(deadSign)
	}()

	fd, err := sock.Accept()
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-time.After(timeout):
			_ = fd.Close()
			_ = sock.Close()
			return
		case <-ticker.C:
			binary.LittleEndian.PutUint32(buff, count)
			_, err = fd.Write(buff)
			if err != nil {
				log.Print(err)
			}
			count++
		}
	}
}
