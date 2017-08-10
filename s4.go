package main

import (
	"bytes"
	"fmt"
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
var DefaultConfig = &S4Config{
	AWSRegion:         "",
	S3Bucket:          "",
	S3Key:             "",
	FlushIntervalTime: time.Minute * 2,
}

// S4Config AWS S3 configurations
type S4Config struct {
	AWSRegion string
	//AWSAccessKey string
	//AWSSecretKey string
	S3Bucket          string
	S3Key             string
	FlushIntervalTime time.Duration
}

// S4 streaming data to AWS S3
type S4 struct {
	db     *leveldb.DB
	s3     *s3.S3
	config *S4Config
	mutex  *sync.Mutex
}

// NewRxS3 RxS3 생성
func NewRxS3(dbPath string, config *S4Config) *S4 {
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

	rxs3 := &S4{
		db:     ldb,
		s3:     s3.New(sess),
		config: config,
		mutex:  &sync.Mutex{},
	}

	return rxs3
}

// WriteBuffer write data to leveldb as buffer
func (rs *S4) WriteBuffer(keyIndex int64, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Print(r)
		}
	}()

	if err := rs.db.Put([]byte(strconv.FormatInt(keyIndex, 10)), data, nil); err != nil {
		log.Panic(err)
	}
}

// BufferConsumer consume all data from leveldb, data will be deleted
func (rs *S4) BufferConsumer() *stream.BytesStream {
	bs := stream.NewBytesStream(stream.NewObserver(nil))
	ticker := time.NewTicker(rs.config.FlushIntervalTime)
	iter := rs.db.NewIterator(nil, nil)
	var corpus []byte

	flush := func() {
		_ = rs.SendToS3(corpus)
	}
	bs.Handler.AtCancel = flush
	bs.Handler.AtComplete = flush

	bs.Target = func() {
	PubLoop:
		for {
			iter = rs.db.NewIterator(nil, nil)
			select {
			case <-bs.AfterCancel():
				break PubLoop
			case <-ticker.C:
				if corpus != nil {
					bs.Send(corpus)
					corpus = nil
				}
			default:
				for iter.Next() {
					corpus = append(corpus, iter.Value()...)
					if err := rs.db.Delete(iter.Key(), nil); err != nil {
						log.Fatal(err)
					}
				}
			}
			iter.Release()
		}
	}

	return bs.Publish(nil)
}

// SendToS3 send data to s3 bucket
func (rs *S4) SendToS3(data []byte) error {
	now := time.Now()
	key := rs.config.S3Key
	timePartition := fmt.Sprintf("%s/year=%d/month=%d/day=%d/%d%d", key, now.Year(), int(now.Month()), now.Day(), now.Hour(), now.Minute())
	obj := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(data)),
		Bucket: aws.String(rs.config.S3Bucket),
		Key:    aws.String(timePartition),
	}

	_, err := rs.s3.PutObject(obj)
	return err
}

// BufferProducer read from unix socket and write to leveldb
func (rs *S4) BufferProducer(sockPath string) *input.UnixSocket {
	var keyIndex int64
	us := input.OpenUnixSocket(sockPath)

	go us.Publish().Subscribe(func(data []byte) {
		rs.WriteBuffer(keyIndex, data)
		keyIndex++
	})

	return us
}
