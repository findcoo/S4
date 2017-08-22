package input

import (
	"bufio"
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
	listen net.Listener
	conn   net.Conn
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
	go us.signalHandler()

	return us
}

// ListenUnixSocket listen unix socket
func ListenUnixSocket(sockPath string) *UnixSocket {
	sock, err := net.Listen("unix", sockPath)
	if err != nil {
		log.Fatal(err)
	}

	fd, err := sock.Accept()
	if err != nil {
		log.Fatal(err)
	}

	obv := stream.NewObserver(stream.DefaultObservHandler())
	bytesStream := stream.NewBytesStream(obv)
	us := &UnixSocket{
		listen:      sock,
		conn:        fd,
		BytesStream: bytesStream,
	}
	go us.signalHandler()

	return us
}

func (us *UnixSocket) shutdown() {
	_ = us.conn.Close()
	if us.listen != nil {
		_ = us.listen.Close()
	}
}

// Publish start observer process and publish the stream read from unix socket
func (us *UnixSocket) Publish() *UnixSocket {
	us.Target = func() {
		scanner := bufio.NewScanner(us.conn)

		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			select {
			case <-us.AfterCancel():
				us.shutdown()
				return
			default:
				data := scanner.Bytes()
				line := append(data, []byte("\n")...)
				us.Send(line)
			}
		}

		if err := scanner.Err(); err != nil {
			if err == io.EOF {
				us.shutdown()
				us.OnComplete()
				return
			}
			log.Fatal(err)
		}
		us.shutdown()
		us.OnComplete()

	}
	us.Watch(nil)

	return us
}

func (us *UnixSocket) signalHandler() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)

	select {
	case state := <-sig:
		log.Printf("capture signal: %s: Close unix socket", state)
		us.shutdown()
	}
}
