package river

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"

	"github.com/findcoo/S4/input"
	"github.com/findcoo/stream"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// JSONRiver handling json data
type JSONRiver struct {
	db     *leveldb.DB
	mutex  *sync.Mutex
	offset uint64
	*Config
}

// NewJSONRiver returns a JSONRiver
func NewJSONRiver(config *Config) *JSONRiver {
	options := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}

	ldb, err := leveldb.OpenFile(config.BufferPath, options)
	if err != nil {
		log.Fatal(err)
	}
	jb := &JSONRiver{
		db:     ldb,
		mutex:  &sync.Mutex{},
		Config: config,
	}
	return jb
}

// Connect wrapping the accept that read a byte slice from the unix server
func (jb *JSONRiver) Connect() *input.UnixSocket {
	return connect(jb.SocketPath, jb.Flow)
}

// Listen wrapping the listen that read a byte slice from the unix client
func (jb *JSONRiver) Listen() *input.UnixSocket {
	return listen(jb.SocketPath, jb.Flow)
}

// Consume consumes a byte slice from levelDB
func (jb *JSONRiver) Consume() *stream.BytesStream {
	var corpus []byte
	iter := jb.db.NewIterator(nil, nil)

	flush := func() {
		_ = jb.Push(corpus)
	}
	bs, ticker := readyConsume(flush, jb.FlushIntervalTime)

	bs.Target = func() {
	PubLoop:
		for {
			iter = jb.db.NewIterator(nil, nil)
			select {
			case <-bs.AfterCancel():
				break PubLoop
			case <-ticker.C:
				jb.mutex.Lock()
				if corpus != nil {
					bs.Send(corpus)
					log.Printf("check offset: %d", jb.offset)
					corpus = nil
					jb.offset = 0
				}
				jb.mutex.Unlock()
			default:
				jb.mutex.Lock()
				for iter.Next() {
					corpus = append(corpus, iter.Value()...)
					if err := jb.db.Delete(iter.Key(), nil); err != nil {
						log.Fatal(err)
					}
				}
				iter.Release()
				if err := iter.Error(); err != nil {
					log.Fatal(err)
				}
				jb.mutex.Unlock()
			}
		}
	}
	return bs.Publish(nil)
}

// Flow writes the byte slice that can be json to LevelDB
func (jb *JSONRiver) Flow(data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Print(r)
		}
	}()

	var validator map[string]interface{}
	if err := json.Unmarshal(data, &validator); err != nil {
		log.Panic(err)
	}

	jb.offset++
	key := strconv.FormatUint(jb.offset, 10)
	if err := jb.db.Put([]byte(key), data, nil); err != nil {
		log.Panic(err)
	}
}
