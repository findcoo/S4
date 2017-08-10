package main

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/findcoo/rxs3/input"
	"github.com/findcoo/rxs3/test"
)

var rs = NewRxS3("./test.db", &S4Config{
	AWSRegion:         "ap-northeast-2",
	S3Bucket:          "test.rxs3",
	S3Key:             "word",
	FlushIntervalTime: time.Second * 1,
})

func TestSendToS3(t *testing.T) {
	rs.config.S3Key = "word"

	if err := rs.SendToS3([]byte("hello s3")); err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestWriteBuffer(t *testing.T) {
	_ = os.Remove("./test.db")
	<-test.UnixTestServer()

	var keyIndex int64
	us := input.OpenUnixSocket("./test.sock")

	us.Publish().Subscribe(func(data []byte) {
		rs.WriteBuffer(keyIndex, data)
		t.Logf("write index:%d, data: %s", keyIndex, data)
		keyIndex++
	})
}

func TestBufferProducer(t *testing.T) {
	<-test.UnixTestServer()

	us := rs.BufferProducer("./test.sock")

	<-time.After(time.Second * 2)
	us.Cancel()
}

func TestReadBuffer(t *testing.T) {
	iter := rs.db.NewIterator(nil, nil)
	for iter.Next() {
		t.Log(iter.Key())
		t.Log(iter.Value())
	}
}

func TestBufferConsumer(t *testing.T) {
	var count int
	buffer := rs.BufferConsumer()

	buffer.Subscribe(func(data []byte) {
		if count > 2 {
			buffer.Cancel()
		}
		log.Print(data)
		count++
	})
}
