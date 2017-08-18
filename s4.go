package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/findcoo/S4/input"
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
	BufferPath        string
	SocketPath        string
}

// S4 streaming data to AWS S3
type S4 struct {
	db     *leveldb.DB
	s3     *s3.S3
	config *S4Config
	mutex  *sync.Mutex
	wo     *opt.WriteOptions
	offset uint64
}

// NewS4 RxS3 생성
func NewS4(config *S4Config) *S4 {
	if config == nil {
		config = DefaultConfig
	}

	options := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}

	ldb, err := leveldb.OpenFile(config.BufferPath, options)
	if err != nil {
		log.Fatal(err)
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(config.AWSRegion),
	})
	if err != nil {
		log.Fatal(err)
	}

	s4 := &S4{
		db:     ldb,
		s3:     s3.New(sess),
		config: config,
		mutex:  &sync.Mutex{},
		wo:     &opt.WriteOptions{Sync: true},
	}

	return s4
}

// WriteBuffer write data to leveldb as buffer
func (sf *S4) WriteBuffer(id uint64, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Print(r)
		}
	}()

	key := strconv.FormatUint(id, 10)
	value := append(data, []byte("\n")...)
	if err := sf.db.Put([]byte(key), value, sf.wo); err != nil {
		log.Panic(err)
	}
}

// BufferConsumer consume all data from leveldb, data will be deleted
func (sf *S4) BufferConsumer() *stream.BytesStream {
	bs := stream.NewBytesStream(stream.NewObserver(nil))
	ticker := time.NewTicker(sf.config.FlushIntervalTime)
	iter := sf.db.NewIterator(nil, nil)
	var corpus []byte

	flush := func() {
		_ = sf.SendToS3(corpus)
	}
	bs.Handler.AtCancel = flush
	bs.Handler.AtComplete = flush

	bs.Target = func() {
	PubLoop:
		for {
			iter = sf.db.NewIterator(nil, nil)
			select {
			case <-bs.AfterCancel():
				break PubLoop
			case <-ticker.C:
				sf.mutex.Lock()
				if corpus != nil {
					bs.Send(corpus)
					log.Printf("check offset: %d", sf.offset)
					corpus = nil
					sf.offset = 0
				}
				sf.mutex.Unlock()
			default:
				sf.mutex.Lock()
				for iter.Next() {
					corpus = append(corpus, iter.Value()...)
					if err := sf.db.Delete(iter.Key(), nil); err != nil {
						log.Fatal(err)
					}
				}
				iter.Release()
				if err := iter.Error(); err != nil {
					log.Fatal(err)
				}
				sf.mutex.Unlock()
			}
		}
	}
	return bs.Publish(nil)
}

// SendToS3 send data to s3 bucket
func (sf *S4) SendToS3(data []byte) error {
	var compressed bytes.Buffer
	gzw := gzip.NewWriter(&compressed)
	_, err := gzw.Write(data)
	if err != nil {
		_ = gzw.Close()
		return err
	}
	_ = gzw.Close()

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	now := time.Now()
	key := sf.config.S3Key
	timePartition := fmt.Sprintf("%s/year=%d/month=%d/day=%d/%s-%2d%2d.gz", key, now.Year(), int(now.Month()), now.Day(), hostname, now.Hour(), now.Minute())
	obj := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(compressed.Bytes())),
		Bucket: aws.String(sf.config.S3Bucket),
		Key:    aws.String(timePartition),
	}

	_, err = sf.s3.PutObject(obj)
	return err
}

// ClientBufferProducer read from unix socket and write to leveldb
func (sf *S4) ClientBufferProducer() *input.UnixSocket {
	us := input.ConnectUnixSocket(sf.config.SocketPath)

	go us.Publish().Subscribe(func(data []byte) {
		sf.offset++
		sf.WriteBuffer(sf.offset, data)
	})
	return us
}

// ServerBufferProducer recieve from client and write to leveldb
func (sf *S4) ServerBufferProducer() *input.UnixSocket {
	us := input.ListenUnixSocket(sf.config.SocketPath)

	go us.Publish().Subscribe(func(data []byte) {
		sf.offset++
		sf.WriteBuffer(sf.offset, data)
	})
	return us
}
