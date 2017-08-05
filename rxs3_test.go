package main

import (
	"os"
	"testing"
	"time"

	"github.com/findcoo/rxs3/input"
	"github.com/findcoo/rxs3/test"
)

func TestWriteBuffer(t *testing.T) {
	_ = os.Remove("./test.db")
	rs := NewRxS3("./test.db")

	<-test.UnixTestServer()

	var keyIndex int64
	us := input.OpenUnixSocket("./test.sock")

	us.Publish().Subscribe(func(data []byte) {
		rs.WriteBuffer(keyIndex, data)
		t.Logf("write index:%d, data: %s", keyIndex, data)
		keyIndex++
	})
}

func TestConsumeBuffer(t *testing.T) {
	rs := NewRxS3("./test.db", nil)

	var count int
	cancel := rs.ConsumeBuffer(func(data []byte) {
		t.Logf("consumed data: %s", data)
		count++
	}, time.Second*1)

	for count > 0 {
		cancel()
	}
}

func TestSendToS3(t *testing.T) {
	rs := NewRxS3("./test.db", &Config{
		AWSRegion: "ap-northeast-2",
		S3Bucket:  "test.rxs3",
		S3Key:     "word",
	})
	if err := rs.SendToS3([]byte("hello s3")); err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestAggregate(t *testing.T) {
	rs := NewRxS3("./test.db", &Config{
		AWSRegion: "ap-northeast-2",
		S3Bucket:  "test.rxs3",
		S3Key:     "corpus",
	})

	cancelUnix, cancelS3 = rs.Aggregate("./test.sock")

}
