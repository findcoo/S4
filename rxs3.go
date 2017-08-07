package main

import (
	"bytes"
	"log"
	"strconv"
	"sync"
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

// DefaultConfig default rxs3 config
var DefaultConfig = &Config{
	AWSRegion:         "",
	S3Bucket:          "",
	S3Key:             "",
	FlushIntervalTime: time.Minute * 2,
}

// Config AWS S3 configurations
type Config struct {
	AWSRegion string
	//AWSAccessKey string
	//AWSSecretKey string
	S3Bucket          string
	S3Key             string
	FlushIntervalTime time.Duration
}

// RxS3 streaming data to AWS S3
type RxS3 struct {
	db     *leveldb.DB
	s3     *s3.S3
	config *Config
	mutex  *sync.Mutex
}

// NewRxS3 RxS3 생성
func NewRxS3(dbPath string, config *Config) *RxS3 {
	if config == nil {
		config = DefaultConfig
	}

	options := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}

	ldb, err := leveldb.OpenFile(dbPath, options)
	if err != nil {
		log.Fatal(err)
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(config.AWSRegion),
	})
	if err != nil {
		log.Fatal(err)
	}

	rxs3 := &RxS3{
		db:     ldb,
		s3:     s3.New(sess),
		config: config,
		mutex:  &sync.Mutex{},
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
	obv := stream.NewObserver(stream.DefaultObservHandler())
	bs := stream.NewBytesStream(obv)
	corpus := []byte("")
	ticker := time.NewTicker(interval)

	flush := func() {
		_ = rs.SendToS3(corpus)
	}
	bs.Handler.AtCancel = flush
	bs.Handler.AtComplete = flush

	bs.Target = func() {
		for {
			iter = rs.db.NewIterator(nil, nil)

			for iter.Next() {
				select {
				case <-bs.Observer.AfterCancel():
					return
				case <-ticker.C:
					rs.mutex.Lock()
					bs.Send(corpus)
					corpus = []byte("")
					rs.mutex.Unlock()
				default:
					rs.mutex.Lock()
					corpus = append(corpus, iter.Value()...)
					if err := rs.db.Delete(iter.Key(), nil); err != nil {
						log.Fatal(err)
					}
					rs.mutex.Unlock()
				}
			}
			iter.Release()
		}
	}

	go bs.Publish(nil).Subscribe(call)
	return bs.Cancel
}

// SendToS3 send data to s3 bucket
func (rs *RxS3) SendToS3(data []byte) error {
	obj := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(data)),
		Bucket: aws.String(rs.config.S3Bucket),
		Key:    aws.String(rs.config.S3Key),
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

		err := rs.SendToS3(data)
		if err != nil {
			log.Panic(err)
		}
	}, rs.config.FlushIntervalTime)

	cancelUnix = unixStream.Cancel
	cancelS3 = cancelConsume
	return
}
