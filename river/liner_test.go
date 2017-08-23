package river

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/findcoo/s4/input"
	"github.com/findcoo/s4/lake"
	"github.com/findcoo/s4/test"
)

var lineRiver = NewLineRiver(&Config{
	BufferPath:        "./tmp",
	SocketPath:        "./line.sock",
	FlushIntervalTime: time.Second * 1,
	Supplyer:          lake.NewConsoleSupplyer(),
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

	time.Sleep(time.Second * 2)
	us.Cancel()
}

func TestLineListen(t *testing.T) {
	go lineRiver.Listen()
	for {
		if _, err := os.Stat(lineRiver.SocketPath); err == nil {
			break
		}
	}

	test.UnixTestClient(lineRiver.SocketPath)

	consumer := lineRiver.Consume()
	consumer.Subscribe(func(data []byte) {
		lineRiver.Push(data)
		consumer.Cancel()
	})
}

func TestLineConsume(t *testing.T) {
	consumer := lineRiver.Consume()

	consumer.Subscribe(func(data []byte) {
		log.Print(data)
		consumer.Cancel()
	})
}
