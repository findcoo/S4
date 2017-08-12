package input

import (
	"bytes"
	"log"
	"os"
	"testing"
	"time"

	"github.com/findcoo/S4/test"
)

func check(data []byte) {
	if ok := bytes.Equal(data, []byte("world")); ok {
		log.Print("end subscribe")
		return
	}

	log.Printf("subscribe data: %s ", string(data))
}

func TestRead(t *testing.T) {
	ready := test.UnixTestServer()
	<-ready
	us := ConnectUnixSocket("./test.sock")

	stream := us.Publish()
	stream.Subscribe(check)
}

func TestListen(t *testing.T) {
	sockPath := "./test.sock"
	go func() {
		us := ListenUnixSocket(sockPath)
		us.Publish().Subscribe(check)
	}()

	for {
		if _, err := os.Stat(sockPath); !os.IsNotExist(err) {
			<-time.After(time.Second * 1)
			test.UnixTestClient(sockPath)
		}
	}
}
