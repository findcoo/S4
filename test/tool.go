package test

import (
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
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

// UnixTestServer unix socket echo server for testing
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

// UnixTestClient unix socket echo client for testing
func UnixTestClient(sockPath string) {
	_ = os.Remove(sockPath)
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		log.Fatal(err)
	}

	echo(conn)
}

// MockUnixEchoServer unix socket mocking server
func MockUnixEchoServer(timeout time.Duration) {
	sock, err := net.Listen("unix", "./mock.sock")
	if err != nil {
		log.Fatal(err)
	}
	var count int
	ticker := time.NewTicker(time.Millisecond * 200)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func() {
		deadSign := <-sig
		_ = sock.Close()
		log.Fatal(deadSign)
	}()

	fd, err := sock.Accept()
	if err != nil {
		log.Fatal(err)
	}

	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			_ = fd.Close()
			_ = sock.Close()
			return
		case <-ticker.C:
			_, err = fd.Write([]byte(strconv.Itoa(count)))
			if err != nil {
				log.Print(err)
			}
			count++
		}
	}
}
