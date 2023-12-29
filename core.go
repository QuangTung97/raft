package raft

import (
	"time"
)

type raftState int

const (
	raftStateFollower raftState = iota + 1
	raftStateCandidate
	raftStateLeader
)

// Raft implements the Raft Consensus Algorithm
type Raft struct {
	storage Storage
	timer   Timer
	client  Client

	state     raftState
	candidate *candidateState

	storageState StorageState
}

type candidateState struct {
	votedNodes map[NodeID]struct{}
}

func NewRaft(storage Storage, timer Timer, client Client) *Raft {
	r := &Raft{
		storage: storage,
		timer:   timer,
		client:  client,

		state: raftStateFollower,
	}

	return r
}

func (r *Raft) isVotedNodesMajority() bool {
	major := len(r.storageState.ClusterNodes)/2 + 1
	if len(r.candidate.votedNodes) >= major {
		return true
	}
	return false
}

func (r *Raft) handleTimeout() {
	r.storageState.CurrentTerm++
	r.storageState.VotedFor = NullNodeID{
		Valid:  true,
		NodeID: r.storageState.NodeID,
	}

	err := r.storage.PutState(r.storageState)
	if err != nil {
		panic(err)
	}

	r.state = raftStateCandidate
	r.candidate = &candidateState{
		votedNodes: map[NodeID]struct{}{
			r.storageState.NodeID: {},
		},
	}

	// do request votes
	for _, id := range r.storageState.ClusterNodes {
		nodeID := id
		if nodeID == r.storageState.NodeID {
			continue
		}

		r.client.RequestVote(RequestVoteInput{
			NodeID:      nodeID,
			Term:        r.storageState.CurrentTerm,
			CandidateID: r.storageState.NodeID,
		}, func(output RequestVoteOutput, err error) {
			r.handleVoteResponse(nodeID, output, err)
		})
	}
}

func (r *Raft) checkResponseTerm(term TermNumber, votedFor NullNodeID) {
	if term > r.storageState.CurrentTerm {
		r.storageState.CurrentTerm = term
		r.storageState.VotedFor = votedFor

		err := r.storage.PutState(r.storageState)
		if err != nil {
			panic(err)
		}

		r.state = raftStateFollower
		r.candidate = nil
	}
}

func (r *Raft) handleVoteResponse(nodeID NodeID, output RequestVoteOutput, err error) {
	r.checkResponseTerm(output.Term, NullNodeID{})
	if r.state != raftStateCandidate {
		return
	}

	if !output.VoteGranted {
		return
	}

	r.candidate.votedNodes[nodeID] = struct{}{}
	if !r.isVotedNodesMajority() {
		return
	}

	r.state = raftStateLeader
	r.candidate = nil

	for _, id := range r.storageState.ClusterNodes {
		destNodeID := id
		if id == r.storageState.NodeID {
			continue
		}
		r.client.AppendEntries(AppendEntriesInput{
			NodeID:   destNodeID,
			Term:     r.storageState.CurrentTerm,
			LeaderID: r.storageState.NodeID,
		}, func(output AppendEntriesOutput, err error) {
		})
	}
}

func (r *Raft) Start() {
	nullState, err := r.storage.GetState()
	if err != nil {
		panic(err)
	}

	r.storageState = nullState.State
	r.timer.AddTimer(10*time.Second, r.handleTimeout)
}

func (r *Raft) RequestVote(input RequestVoteInput) RequestVoteOutput {
	r.checkResponseTerm(input.Term, NullNodeID{
		Valid:  true,
		NodeID: input.CandidateID,
	})

	granted := false
	votedFor := r.storageState.VotedFor
	if votedFor.Valid && votedFor.NodeID == input.CandidateID {
		granted = true
	}

	return RequestVoteOutput{
		Term:        input.Term,
		VoteGranted: granted,
	}
}
