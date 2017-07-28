package main

import (
	"os"
	"testing"

	"github.com/findcoo/rxs3/input"
)

func TestRxs3(t *testing.T) {
	_ := os.Remove("./test.db")
	rs := NewRxS3("./test.db")

	unixSock := &input.NewUnixSocket{}
	rs.BufferInput()
}
