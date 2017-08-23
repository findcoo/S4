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

var jsonRiver = NewJSONRiver(&Config{
	BufferPath:        "./test.db",
	SocketPath:        "./json.sock",
	FlushIntervalTime: time.Second * 1,
	Supplyer:          lake.NewS3Supplyer("ap-northeast-2", "test.s4", "json"),
})

func lockUntilReady(socketpath string) {
	for {
		if _, err := os.Stat(socketpath); err == nil {
			break
		}
		time.Sleep(time.Millisecond * 200)
	}
}

func TestJSONFlow(t *testing.T) {
	<-test.UnixTestServer(jsonRiver.SocketPath)

	var key uint64
	us := input.ConnectUnixSocket(jsonRiver.SocketPath)

	us.Publish().Subscribe(func(data []byte) {
		jsonRiver.Flow(data)
		t.Logf("write index:%d, data: %s", jsonRiver.offset, data)
		key++
	})
}

func TestJSONConnect(t *testing.T) {
	<-test.UnixTestServer(jsonRiver.SocketPath)
	us := jsonRiver.Connect()
	time.Sleep(time.Second * 2)
	us.Cancel()
}

func TestReadLevelDB(t *testing.T) {
	iter := jsonRiver.db.NewIterator(nil, nil)

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

func TestJSONConsume(t *testing.T) {
	consumer := jsonRiver.Consume()

	consumer.Subscribe(func(data []byte) {
		log.Print(data)
		consumer.Cancel()
	})
}

func BenchmarkJSON(b *testing.B) {
	ready, _ := test.UnixBenchmarkServer(10, jsonRiver.SocketPath)
	<-ready
	jsonRiver.Connect()

	consumer := jsonRiver.Consume()
	consumer.Subscribe(func(data []byte) {
		log.Print(string(data))
	})
}
