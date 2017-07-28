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
	out := us.Subscribe()

	for {
		data := <-out

		if ok := bytes.Equal(data, []byte("world")); ok {
			log.Print("end subscribe")
			return
		}

		log.Printf("subscribe data: %s ", string(data))
	}
}
