package main

import (
	"log"
	"testing"
	"time"

	"github.com/findcoo/S4/input"
	"github.com/findcoo/S4/test"
)

var s4 = NewS4(&S4Config{
	AWSRegion:         "ap-northeast-2",
	S3Bucket:          "test.s4",
	S3Key:             "word",
	FlushIntervalTime: time.Second * 1,
	BufferPath:        "./test.db",
	SocketPath:        "./test.sock",
})

func TestSendToS3(t *testing.T) {
	s4.config.S3Key = "word"

	if err := s4.SendToS3([]byte("hello s3")); err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestWriteBuffer(t *testing.T) {
	<-test.UnixTestServer()

	var key uint64
	us := input.ConnectUnixSocket(s4.config.SocketPath)

	us.Publish().Subscribe(func(data []byte) {
		s4.WriteBuffer(key, data)
		t.Logf("write index:%d, data: %s", key, data)
		key++
	})
}

func TestBufferProducer(t *testing.T) {
	<-test.UnixTestServer()

	us := s4.ClientBufferProducer()

	<-time.After(time.Second * 2)
	us.Cancel()
}

func TestReadBuffer(t *testing.T) {
	iter := s4.db.NewIterator(nil, nil)

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

func TestBufferConsumer(t *testing.T) {
	buffer := s4.BufferConsumer()

	buffer.Subscribe(func(data []byte) {
		log.Print(data)
		buffer.Cancel()
	})
}

func BenchmarkS4(b *testing.B) {
	var s4 = NewS4(&S4Config{
		AWSRegion:         "ap-northeast-2",
		S3Bucket:          "test.s4",
		S3Key:             "word",
		FlushIntervalTime: time.Second * 1,
		BufferPath:        "./bench.db",
		SocketPath:        "./bench.sock",
	})

	ready, _ := test.UnixBenchmarkServer(10)
	<-ready
	s4.ClientBufferProducer()

	consumer := s4.BufferConsumer()
	consumer.Subscribe(func(data []byte) {
		log.Print(string(data))
	})
}
