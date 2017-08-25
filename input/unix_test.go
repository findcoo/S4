package input

import (
	"bytes"
	"log"
	"testing"

	"github.com/findcoo/s4/test"
)

func check(data []byte) {
	if ok := bytes.Equal(data, []byte("world")); ok {
		log.Print("end subscribe")
		return
	}

	log.Printf("subscribe data: %s ", string(data))
}

func TestRead(t *testing.T) {
	ready := test.UnixTestServer("./test.sock")
	<-ready
	us := ConnectUnixSocket("./test.sock")

	stream := us.Publish()
	stream.Subscribe(check)
}

func TestListen(t *testing.T) {
	sockPath := "./listen.sock"
	streams, stop := ListenUnixSocket(sockPath)
	test.LockUntilReady(sockPath)
	test.UnixTestClient(sockPath)

	us := <-streams
	us.Publish().Subscribe(check)
	stop()
}

func BenchmarkUnix(b *testing.B) {
	iterN := 100
	ready, _ := test.UnixBenchmarkServer(iterN, "./bench.sock")
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
