package input

import (
	"bufio"
	"io"
	"log"
	"net"

	"github.com/findcoo/stream"
)

// UnixSocket read data from unix socket
type UnixSocket struct {
	conn net.Conn
	*stream.BytesStream
}

// ConnectUnixSocket connect unix socket
func ConnectUnixSocket(sockPath string) *UnixSocket {
	c, err := net.Dial("unix", sockPath)
	if err != nil {
		log.Fatal(err)
	}

	obv := stream.NewObserver(stream.DefaultObservHandler())
	bytesStream := stream.NewBytesStream(obv)
	us := &UnixSocket{
		conn:        c,
		BytesStream: bytesStream,
	}
	obv.Handler.AtCancel = us.shutdown
	obv.Handler.AtComplete = us.shutdown
	return us
}

func acceptAfter(sock net.Listener) <-chan net.Conn {
	pipe := make(chan net.Conn, 1)
	go func() {
		fd, err := sock.Accept()
		if err != nil {
			return
		}
		pipe <- fd
	}()
	return pipe
}

// ListenUnixSocket returns a UnixSocket channel
func ListenUnixSocket(sockPath string) (<-chan *UnixSocket, func()) {
	streams := make(chan *UnixSocket, 1)
	done := make(chan struct{}, 1)
	stop := func() {
		done <- struct{}{}
	}

	go func() {
		sock, err := net.Listen("unix", sockPath)
		if err != nil {
			log.Fatal(err)
		}

	ServerLoop:
		for {
			select {
			case <-done:
				_ = sock.Close()
				break ServerLoop
			case fd := <-acceptAfter(sock):
				log.Print("accept client")
				obv := stream.NewObserver(stream.DefaultObservHandler())
				bytesStream := stream.NewBytesStream(obv)
				us := &UnixSocket{
					conn:        fd,
					BytesStream: bytesStream,
				}
				obv.Handler.AtComplete = us.shutdown
				streams <- us
			}
		}
	}()
	return streams, stop
}

func (us *UnixSocket) shutdown() {
	_ = us.conn.Close()
}

// Publish observ and publish the stream that read from the unix socket
func (us *UnixSocket) Publish() *UnixSocket {
	us.Target = func() {
		scanner := bufio.NewScanner(us.conn)

		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			select {
			case <-stream.AfterSignal():
				return
			case <-us.AfterCancel():
				return
			default:
				data := scanner.Bytes()
				line := append(data, []byte("\n")...)
				us.Send(line)
			}
		}

		if err := scanner.Err(); err != nil {
			if err == io.EOF {
				us.OnComplete()
				return
			}
			log.Fatal(err)
		}
		us.OnComplete()

	}
	us.Watch(nil)
	return us
}
