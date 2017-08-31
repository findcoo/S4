package lake

import (
	"testing"
)

func TestS3Supplyer(t *testing.T) {
	s3 := NewS3Supplyer("ap-northeast-2", "test.quicket.s4", "testresult")
	s3.Push([]byte("hello world, this is s3 supplyer test"))
}
