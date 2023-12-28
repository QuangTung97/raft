package raft

import (
	"testing"
)

type raftTest struct {
	client *ClientMock
	r      *Raft
}

func newRaftTest() *raftTest {
	r := &raftTest{}

	r.r = NewRaft(r.client)
	return r
}

func TestRaft(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		newRaftTest()
	})
}
