package main

import (
	"os"
	"testing"

	"github.com/findcoo/rxs3/input"
	"github.com/findcoo/rxs3/test"
)

func TestRxs3(t *testing.T) {
	_ = os.Remove("./test.db")
	rs := NewRxS3("./test.db")

	<-test.UnixTestServer()

	us := input.OpenUnixSocket("./test.sock")
	rs.BufferInput(us)
}
