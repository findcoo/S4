package main

import (
	"bytes"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/findcoo/rxs3/input"
	"github.com/findcoo/stream"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var delim = []byte("[:delim:]")

// RxS3 streaming data to AWS S3
type RxS3 struct {
	db *leveldb.DB
	s3 *s3.S3
}

// NewRxS3 RxS3 생성
func NewRxS3(dbPath string) *RxS3 {
	options := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}

	ldb, err := leveldb.OpenFile(dbPath, options)
	if err != nil {
		log.Fatal(err)
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-2"),
	})
	if err != nil {
		log.Fatal(err)
	}

	rxs3 := &RxS3{
		db: ldb,
		s3: s3.New(sess),
	}

	return rxs3
}

// WriteBuffer write data to leveldb as buffer
func (rs *RxS3) WriteBuffer(keyIndex int64, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Print(r)
		}
	}()

	if err := rs.db.Put([]byte(strconv.FormatInt(keyIndex, 10)), data, nil); err != nil {
		log.Panic(err)
	}
}

// ConsumeBuffer consume all data from leveldb, data will be deleted
func (rs *RxS3) ConsumeBuffer(call func([]byte), interval time.Duration) func() {
	iter := rs.db.NewIterator(nil, nil)
	bs := stream.NewBytesStream(stream.DefaultObservHandler())

	bs.Observer.SetObservable(func() {
		ticker := time.NewTicker(interval)

		for {
			for iter.Next() {
				select {
				case <-bs.Observer.AfterCancel():
					return
				default:
					data := bytes.Join([][]byte{iter.Key(), iter.Value()}, delim)
					bs.Send(data)
				}
			}
			iter.Release()
			<-ticker.C
		}
	})

	go bs.Publish().Subscribe(call)
	return bs.Cancel
}

// SendToS3 send data to s3 bucket
func (rs *RxS3) SendToS3(data []byte) error {
	obj := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(data)),
		Bucket: aws.String("quicket-log-house"),
		Key:    aws.String("suricata/test"),
	}

	_, err := rs.s3.PutObject(obj)
	return err
}

// Aggregate aggregate log between unix socket and s3
// NOTE first return value cancel read from unix socket, second return value cancel aggregate to s3
func (rs *RxS3) Aggregate(sockPath string) (cancelUnix func(), cancelS3 func()) {
	var keyIndex int64
	us := input.OpenUnixSocket(sockPath)
	unixStream := us.Publish()

	go unixStream.Subscribe(func(data []byte) {
		rs.WriteBuffer(keyIndex, data)
		keyIndex++
	})

	cancelConsume := rs.ConsumeBuffer(func(data []byte) {
		defer func() {
			if r := recover(); r != nil {
				log.Print(r)
			}
		}()

		splited := bytes.Split(data, delim)
		if len(splited) != 2 {
			log.Print("Invalid data, isn`t there delimeter?")
			if err := rs.db.Delete(splited[0], nil); err != nil {
				log.Fatal(err)
			}
			return
		}

		key, value := splited[0], splited[1]

		err := rs.SendToS3(value)
		if err != nil {
			log.Panic(err)
		}

		if err := rs.db.Delete(key, nil); err != nil {
			log.Fatal(err)
		}
	}, time.Minute*2)

	cancelUnix = unixStream.Cancel
	cancelS3 = cancelConsume
	return
}
