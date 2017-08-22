package lake

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Supplyer data-lake interface
type Supplyer interface {
	Push(data []byte) error
}

// S3Supplyer AWS S3 data-lake
type S3Supplyer struct {
	Bucket string
	Key    string
	client *s3.S3
}

// ConsoleSupplyer commonly use for debugging
type ConsoleSupplyer struct {
	stdout *os.File
}

// NewConsoleSupplyer returns a ConsoleSupplyer
func NewConsoleSupplyer() *ConsoleSupplyer {
	console := &ConsoleSupplyer{
		stdout: os.Stdout,
	}
	return console
}

// Push print a byte slice
func (cs *ConsoleSupplyer) Push(data []byte) error {
	_, err := cs.stdout.Write(data)
	return err
}

// NewS3Supplyer create s3 client
func NewS3Supplyer(region, bucket, key string) *S3Supplyer {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		log.Fatal(err)
	}

	s3supplyer := &S3Supplyer{
		Bucket: bucket,
		Key:    key,
		client: s3.New(sess),
	}
	return s3supplyer
}

// Push push data to s3 bucket
func (sl *S3Supplyer) Push(data []byte) error {
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
	key := sl.Key
	timePartition := fmt.Sprintf("%s/year=%d/month=%d/day=%d/%s-%2d%2d.gz", key, now.Year(), int(now.Month()), now.Day(), hostname, now.Hour(), now.Minute())
	obj := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(compressed.Bytes())),
		Bucket: aws.String(sl.Bucket),
		Key:    aws.String(timePartition),
	}

	_, err = sl.client.PutObject(obj)
	return err
}
