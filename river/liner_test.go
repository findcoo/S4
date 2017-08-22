package river

import (
	"log"
	"testing"
	"time"

	"github.com/findcoo/S4/input"
	"github.com/findcoo/S4/lake"
	"github.com/findcoo/S4/test"
)

var lineRiver = NewLineRiver(&Config{
	BufferPath:        "",
	SocketPath:        "./line.sock",
	FlushIntervalTime: time.Second * 1,
	Supplyer:          lake.NewS3Supplyer("ap-northeast-2", "test.s4", "line"),
})

func TestLineFlow(t *testing.T) {
	<-test.UnixTestServer(lineRiver.SocketPath)

	var key uint64
	us := input.ConnectUnixSocket(lineRiver.SocketPath)

	us.Publish().Subscribe(func(data []byte) {
		lineRiver.Flow(data)
		t.Logf("write index:%d, data: %s", key, data)
		key++
	})
}

func TestLineConnect(t *testing.T) {
	<-test.UnixTestServer(lineRiver.SocketPath)

	us := lineRiver.Connect()

	<-time.After(time.Second * 2)
	us.Cancel()
}

func TestLineConsume(t *testing.T) {
	consumer := lineRiver.Consume()

	consumer.Subscribe(func(data []byte) {
		log.Print(data)
		consumer.Cancel()
	})
}
