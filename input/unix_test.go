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
	sockPath := "./listen.sock"
	go func() {
		us := ListenUnixSocket(sockPath)
		us.Publish().Subscribe(check)
	}()

	for {
		time.Sleep(time.Second * 1)
		if _, err := os.Stat(sockPath); err == nil {
			test.UnixTestClient(sockPath)
			break
		}
	}
}

func BenchmarkUnix(b *testing.B) {
	iterN := 100
	ready, _ := test.UnixBenchmarkServer(iterN)
	<-ready
	us := ConnectUnixSocket("./bench.sock")

	var counter int
	stream := us.Publish()
	stream.Subscribe(func(data []byte) {
		log.Print(string(data))
		counter++
		if iterN == counter {
			b.Logf("count %d", counter)
			stream.Cancel()
		}
	})
}
