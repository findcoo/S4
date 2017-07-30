package input

// ByteStream  byte데이터만 흐르는 stream
type ByteStream struct {
	stream       chan []byte
	Observer     *Observer
	Subscribable func(data []byte)
}

// Observer Status를 모니터링하는 관찰자 객체
type Observer struct {
	done    chan struct{}
	err     chan error
	cancel  chan struct{}
	end     chan struct{}
	Handler Handler
}

// Handler Observer의 상태 변화에 따라 실행될 핸들러
type Handler struct {
	Observalble func()
	AtComplete  func()
	AtStop      func()
	AtError     func(err error)
}

// NewObserver Observer 생성
func NewObserver() *Observer {
	obv := &Observer{
		done:   make(chan struct{}, 1),
		err:    make(chan error, 1),
		cancel: make(chan struct{}, 1),
		end:    make(chan struct{}, 1),
	}

	return obv
}

// NewByteStream ByteStream 생성
func NewByteStream() *ByteStream {
	bs := &ByteStream{
		stream:   make(chan []byte, 1),
		Observer: NewObserver(),
	}

	return bs
}

// Observ 대상 핸들러를 관찰한다.
func (o *Observer) Observ() {
	go func() {
		for {
			select {
			case <-o.done:
				o.Handler.AtComplete()
				return
			case <-o.cancel:
				o.Handler.AtStop()
				o.end <- struct{}{}
				return
			case err := <-o.err:
				o.Handler.AtError(err)
			default:
				o.Handler.Observalble()
			}
		}
	}()
}

// OnComplete Observer의 활동을 끝낸다.
func (o *Observer) OnComplete() {
	o.done <- struct{}{}
}

// OnError error를 전달한다.
func (o *Observer) OnError(err error) {
	o.err <- err
}

// Send 데이터를 전송한다.
func (bs *ByteStream) Send(data []byte) {
	bs.stream <- data
}

// Publish 관찰중인 stream을 발행한다.
func (bs *ByteStream) Publish() *ByteStream {
	bs.Observer.Observ()

	return bs
}

// Subscribe 데이터를 구독
func (bs *ByteStream) Subscribe(callArray ...func(data []byte)) {
	go func() {
		for {
			select {
			case <-bs.Observer.end:
				return
			case data := <-bs.stream:
				if len(callArray) != 0 {
					for _, call := range callArray {
						call(data)
					}
				} else {
					bs.Subscribable(data)
				}
			}
		}
	}()
}
