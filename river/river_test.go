package river

import (
	"log"
	"testing"
	"time"

	"github.com/findcoo/S4/input"
	"github.com/findcoo/S4/lake"
	"github.com/findcoo/S4/test"
)

var jsonBuffer = NewJSONRiver(&Config{
	BufferPath:        "./test.db",
	SocketPath:        "./test.sock",
	FlushIntervalTime: time.Second * 1,
	Lake:              lake.NewS3Lake("ap-northeast-2", "test.s4", "word"),
})

func TestJSONBufferWrite(t *testing.T) {
	<-test.UnixTestServer()

	var key uint64
	us := input.ConnectUnixSocket(jsonBuffer.SocketPath)

	us.Publish().Subscribe(func(data []byte) {
		jsonBuffer.Flow(data)
		t.Logf("write index:%d, data: %s", jsonBuffer.offset, data)
		key++
	})
}

func TestJSONBufferClientProduce(t *testing.T) {
	<-test.UnixTestServer()

	us := jsonBuffer.Accept()

	<-time.After(time.Second * 2)
	us.Cancel()
}

func TestReadBuffer(t *testing.T) {
	iter := jsonBuffer.db.NewIterator(nil, nil)

	for iter.Next() {
		t.Log(string(iter.Key()))
		t.Log(string(iter.Value()))
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestJSONBufferConsume(t *testing.T) {
	consumer := jsonBuffer.Consume()

	consumer.Subscribe(func(data []byte) {
		log.Print(data)
		consumer.Cancel()
	})
}

func BenchmarkS4(b *testing.B) {
	ready, _ := test.UnixBenchmarkServer(10, jsonBuffer.SocketPath)
	<-ready
	jsonBuffer.Accept()

	consumer := jsonBuffer.Consume()
	consumer.Subscribe(func(data []byte) {
		log.Print(string(data))
	})
}
