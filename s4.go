package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/findcoo/S4/input"
	"github.com/findcoo/stream"
	"github.com/google/uuid"
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

// NewS4 RxS3 생성
func NewS4(dbPath string, config *S4Config) *S4 {
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
func (rs *S4) WriteBuffer(id uint32, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Print(r)
		}
	}()

	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, id)
	debug := binary.LittleEndian.Uint32(key)

	log.Printf("uuid %d", debug)
	if err := rs.db.Put(key, data, nil); err != nil {
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
	key := rs.config.S3Key
	timePartition := fmt.Sprintf("%s/year=%d/month=%d/day=%d/%s-%d%d.gz", key, now.Year(), int(now.Month()), now.Day(), hostname, now.Hour(), now.Minute())
	obj := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(compressed.Bytes())),
		Bucket: aws.String(rs.config.S3Bucket),
		Key:    aws.String(timePartition),
	}

	_, err = rs.s3.PutObject(obj)
	return err
}

// BufferProducer read from unix socket and write to leveldb
func (rs *S4) BufferProducer(sockPath string) *input.UnixSocket {
	us := input.ConnectUnixSocket(sockPath)

	go us.Publish().Subscribe(func(data []byte) {
		id := uuid.New().ID()
		rs.WriteBuffer(id, data)
	})

	return us
}
