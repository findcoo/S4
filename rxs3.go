package main

import (
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/findcoo/rxs3/input"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// RxS3 buffer로 부터 데이터를 읽고 s3에 쓰는 객체
// NOTE 버퍼로 leveldb를 사용한다.
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

	sess := session.New()

	rxs3 := &RxS3{
		db: ldb,
		s3: s3.New(sess),
	}

	return rxs3
}

// BufferInput buffer db에 정보를 누적한다.
func (rs *RxS3) BufferInput(input input.Input) {
	var keyIndex int
	out := input.Read()

	for {
		select {
		case data := <-out:
			if err := rs.db.Put([]byte(strconv.Itoa(keyIndex)), data, nil); err != nil {
				log.Panic(err)
			}
			keyIndex++
		}
	}
}
