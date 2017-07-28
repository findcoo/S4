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
	conn   net.Conn
	stream *Streams
}

type Streams struct {
	line chan []byte
	done chan struct{}
}

// OpenUnixSocket UnixSocket을 생성하고 소켓과 연결한다.
func OpenUnixSocket(sockPath string) *UnixSocket {
	c, err := net.Dial("unix", sockPath)
	if err != nil {
		log.Fatal(err)
	}

	streams := &Streams{
		Line:   make(chan []byte, 1),
		Closed: make(chan struct{}, 1),
	}

	us := &UnixSocket{
		conn:   c,
		stream: streams,
	}
	go us.signalHandler()

	return us
}

// Subscribe socket으로 데이터를 읽음
// channel을 반환한다.
func (us *UnixSocket) Publish() *Streams {
	go func() {
		for {
			buff := make([]byte, 4096)

			_, err := us.conn.Read(buff)
			if err != nil {
				if err == io.EOF {
					_ = us.conn.Close()
					us.stream.closed <- struct{}{}
					return
				}

				log.Fatal(err)
			}

			us.stream.line <- bytes.Trim(buff, "\x00")
		}
	}()

	return &us.stream
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

func (s *Streams) Subscribe() <-chan []byte {
	return s.line
}

func (s *Streams) Done() <-chan struct{} {
	return s.done
}

// TODO 고루틴에 락을 걸어야됨
func (s *Streams) Unsubscribe() {
	close(s.line)
}
