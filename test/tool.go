package test

import (
	"fmt"
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
			_, err = c.Write([]byte(`{"message": "world"}` + "\n"))
			_ = c.Close()
			return true
		}
		_, err = c.Write([]byte(`{"message": "hello"}` + "\n"))
		if err != nil {
			log.Fatal(err)
		}
	}

	return false
}

// UnixTestServer unix socket echo server for testing
func UnixTestServer() <-chan struct{} {
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
				_ = sock.Close()
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

// UnixBenchmarkServer benchmark unix server
func UnixBenchmarkServer(n int, sockpath string) (<-chan struct{}, <-chan struct{}) {
	ready := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	sock, err := net.Listen("unix", sockpath)
	if err != nil {
		log.Fatal(err)
	}
	ready <- struct{}{}
	go func() {
		fd, err := sock.Accept()
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i <= n; i++ {
			_, err := fd.Write([]byte(fmt.Sprintf("benchmaking: %d\n", i)))
			if err != nil {
				log.Fatal(err)
			}
		}
		_ = fd.Close()
		_ = sock.Close()
		done <- struct{}{}
	}()
	return ready, done
}

// UnixTestClient unix socket echo client for testing
func UnixTestClient(sockPath string) {
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
			msg := fmt.Sprintf(`{"index": "%d"}`, count)
			_, err = fd.Write([]byte(msg))
			if err != nil {
				log.Print(err)
			}
			count++
		}
	}
}
