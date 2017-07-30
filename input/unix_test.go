package input

import (
	"bytes"
	"log"
	"testing"

	"github.com/findcoo/rxs3/test"
)

func TestRead(t *testing.T) {
	ready := test.UnixTestServer()
	<-ready
	us := OpenUnixSocket("./test.sock")

	test1 := func(data []byte) {
		if ok := bytes.Equal(data, []byte("world")); ok {
			log.Print("end subscribe")
			return
		}

		log.Printf("subscribe data: %s ", string(data))
	}

	stream := us.Publish()
	stream.Subscribe(test1)
}
