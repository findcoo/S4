package input

import (
	"bytes"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/findcoo/stream"
)

// UnixSocket read data from unix socket
type UnixSocket struct {
	conn   net.Conn
	stream *stream.BytesStream
}

// OpenUnixSocket open unix socket
func OpenUnixSocket(sockPath string) *UnixSocket {
	c, err := net.Dial("unix", sockPath)
	if err != nil {
		log.Fatal(err)
	}

	obv := stream.NewObserver(stream.DefaultObservHandler())
	bytesStream := stream.NewBytesStream(obv)
	us := &UnixSocket{
		conn:   c,
		stream: bytesStream,
	}
	go us.signalHandler()

	return us
}

// Publish start observer process and publish the stream read from unix socket
func (us *UnixSocket) Publish() *stream.BytesStream {
	us.stream.Target = func() {
		buff := make([]byte, 1024)

		for {
			_, err := us.conn.Read(buff)
			if err != nil {
				if err == io.EOF {
					_ = us.conn.Close()
					us.stream.Observer.OnComplete()
					return
				}
				log.Fatal(err)
			}

			select {
			case <-us.stream.Observer.AfterCancel():
				return
			default:
				us.stream.Send(bytes.Trim(buff, "\x00"))
			}
		}
	}

	return us.stream.Publish(nil)
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
