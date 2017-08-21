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

// Lake data-lake interface
type Lake interface {
	Push(data []byte) error
}

// S3Lake AWS S3 data-lake
type S3Lake struct {
	Bucket string
	Key    string
	client *s3.S3
}

// NewS3Lake create s3 client
func NewS3Lake(region, bucket, key string) *S3Lake {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		log.Fatal(err)
	}

	s3lake := &S3Lake{
		Bucket: bucket,
		Key:    key,
		client: s3.New(sess),
	}
	return s3lake
}

// Push push data to s3 bucket
func (sl *S3Lake) Push(data []byte) error {
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
