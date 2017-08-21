package river

import (
	"time"

	"github.com/findcoo/S4/input"
	"github.com/findcoo/S4/lake"
	"github.com/findcoo/stream"
)

// River meaning temporary data-stream flow to data-lake
type River interface {
	Accept() *input.UnixSocket
	Listen() *input.UnixSocket
	Consume() *stream.BytesStream
	Flow(data []byte)
}

// Config ...
type Config struct {
	BufferPath        string
	SocketPath        string
	FlushIntervalTime time.Duration
	lake.Lake
}

func connect(sockpath string, flowFunc func([]byte)) *input.UnixSocket {
	us := input.ConnectUnixSocket(sockpath)

	go us.Publish().Subscribe(func(data []byte) {
		flowFunc(data)
	})
	return us
}

func listen(sockpath string, flowFunc func([]byte)) *input.UnixSocket {
	us := input.ListenUnixSocket(sockpath)

	go us.Publish().Subscribe(func(data []byte) {
		flowFunc(data)
	})
	return us
}

func readyConsume(flush func(), flushtime time.Duration) (*stream.BytesStream, *time.Ticker) {
	bs := stream.NewBytesStream(stream.NewObserver(nil))
	ticker := time.NewTicker(flushtime)

	bs.Handler.AtCancel = flush
	bs.Handler.AtComplete = flush
	return bs, ticker
}
