package river

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

	"github.com/findcoo/S4/input"
	"github.com/findcoo/stream"
)

// LineRiver flow with "\n"
type LineRiver struct {
	file  *os.File
	mutex *sync.Mutex
	*Config
}

func defaultBufferPath() string {
	return os.Getenv("HOME") + "/.s4/tmp"
}

// NewLineRiver returns a LineRiver
func NewLineRiver(config *Config) *LineRiver {
	if config.BufferPath == "" {
		config.BufferPath = defaultBufferPath()
	}

	dirname := path.Dir(config.BufferPath)
	if err := os.Mkdir(dirname, 0755); err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	f, err := os.OpenFile(config.BufferPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}

	lr := &LineRiver{
		file:   f,
		mutex:  &sync.Mutex{},
		Config: config,
	}
	return lr
}

// Connect wrapping the accept
func (lr *LineRiver) Connect() *input.UnixSocket {
	return connect(lr.SocketPath, lr.Flow)
}

// Listen wrapping the listen
func (lr *LineRiver) Listen() *input.UnixSocket {
	return listen(lr.SocketPath, lr.Flow)
}

// Consume returns the *stream.BytesStream
func (lr *LineRiver) Consume() *stream.BytesStream {
	flush := func() {
		data, _ := ioutil.ReadAll(lr.file)
		_ = lr.Push(data)
	}
	bs, ticker := readyConsume(flush, lr.FlushIntervalTime)

	bs.Target = func() {
	PubLoop:
		for {
			select {
			case <-bs.AfterCancel():
				break PubLoop
			case <-ticker.C:
				data, err := ioutil.ReadAll(lr.file)
				if err != nil {
					log.Fatal(err)
				}
				bs.Send(data)
				if err := lr.file.Truncate(0); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
	return bs.Publish(nil)
}

// Flow writes a byte slice to file buffer
func (lr *LineRiver) Flow(data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Print(r)
		}
	}()

	if _, err := lr.file.Write(data); err != nil {
		log.Panic(err)
	}
}
