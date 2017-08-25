package river

import (
	"log"
	"time"

	"github.com/findcoo/s4/input"
	"github.com/findcoo/s4/lake"
	"github.com/findcoo/stream"
)

// River meaning temporary data-stream flow to the data-lake
type River interface {
	Connect() *input.UnixSocket
	Listen() func()
	Consume() *stream.BytesStream
	Flow(data []byte)
	lake.Supplyer
}

// Config ...
type Config struct {
	BufferPath        string
	SocketPath        string
	FlushIntervalTime time.Duration
	lake.Supplyer
}

func connect(sockpath string, flowFunc func([]byte)) *input.UnixSocket {
	log.Print("Connect to the waterhead")
	us := input.ConnectUnixSocket(sockpath)

	go us.Publish().Subscribe(func(data []byte) {
		flowFunc(data)
	})
	return us
}

func listen(sockpath string, flowFunc func([]byte)) func() {
	log.Print("Listenning")
	streams, stop := input.ListenUnixSocket(sockpath)
	go func() {
		for us := range streams {
			us.Publish().Subscribe(func(data []byte) {
				flowFunc(data)
			})
		}
	}()
	return stop
}

func readyConsume(flush func(), flushtime time.Duration) (*stream.BytesStream, *time.Ticker) {
	log.Print("Consume the flow")
	bs := stream.NewBytesStream(stream.NewObserver(nil))
	ticker := time.NewTicker(flushtime)

	bs.Handler.AtCancel = flush
	return bs, ticker
}
