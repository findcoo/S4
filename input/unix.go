package input

import (
	"bytes"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

// UnixSocket 유닉스 소켓으로 부터 데이터를 읽고
// 읽은 데이터를 버퍼에 쓰는 객체
type UnixSocket struct {
	conn net.Conn
}

// OpenUnixSocket UnixSocket을 생성하고 소켓과 연결한다.
func OpenUnixSocket(sockPath string) *UnixSocket {
	c, err := net.Dial("unix", sockPath)
	if err != nil {
		log.Fatal(err)
	}

	us := &UnixSocket{conn: c}
	go us.signalHandler()

	return us
}

// Subscribe socket으로 데이터를 읽음
// channel을 반환한다.
func (us *UnixSocket) Subscribe() <-chan []byte {
	in := make(chan []byte, 1)

	go func() {
		for {
			buff := make([]byte, 4096)

			_, err := us.conn.Read(buff)
			if err != nil {
				if err == io.EOF {
					_ = us.conn.Close()
					return
				}

				log.Fatal(err)
			}

			in <- bytes.Trim(buff, "\x00")
		}
	}()

	return in
}

func (us *UnixSocket) signalHandler() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)

	select {
	case state := <-sig:
		log.Printf("capture signal: %s: Close unix socket", state)
		_ = us.conn.Close()
	}
}
