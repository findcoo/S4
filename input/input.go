package input

// Input 데이터를 읽어오는 클라이언트 인터페이스
type Input interface {
	Subscribe() <-chan []byte
}
