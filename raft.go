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
	leader    *leaderState

	storageState StorageState
}

type candidateState struct {
	votedNodes    map[NodeID]struct{}
	rejectedNodes map[NodeID]struct{}
	errorNodes    map[NodeID]struct{}
}

type nodeIndexState struct {
	nextIndex  LogIndex
	inProgress bool
	matchIndex LogIndex
}

type leaderState struct {
	nodeIndices map[NodeID]*nodeIndexState
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
	return r.isMajority(r.candidate.votedNodes)
}

func (r *Raft) isMajority(nodeSet map[NodeID]struct{}) bool {
	major := len(r.storageState.ClusterNodes)/2 + 1
	if len(nodeSet) >= major {
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

	r.storage.PutState(r.storageState)

	r.state = raftStateCandidate
	r.candidate = &candidateState{
		votedNodes: map[NodeID]struct{}{
			r.storageState.NodeID: {},
		},
		rejectedNodes: map[NodeID]struct{}{},
		errorNodes:    map[NodeID]struct{}{},
	}

	// do request votes
	for _, id := range r.storageState.ClusterNodes {
		nodeID := id
		if nodeID == r.storageState.NodeID {
			continue
		}
		r.callRequestVote(nodeID)
	}
}

func (r *Raft) callRequestVote(nodeID NodeID) {
	r.client.RequestVote(RequestVoteInput{
		NodeID:      nodeID,
		Term:        r.storageState.CurrentTerm,
		CandidateID: r.storageState.NodeID,
	}, func(output RequestVoteOutput, err error) {
		r.handleVoteResponse(nodeID, output, err)
	})
}

func (r *Raft) checkResponseTerm(term TermNumber, votedFor NullNodeID) {
	if term > r.storageState.CurrentTerm {
		r.storageState.CurrentTerm = term
		r.storageState.VotedFor = votedFor

		r.storage.PutState(r.storageState)

		r.state = raftStateFollower
		r.candidate = nil
	}
}

func (r *Raft) candidateCheckedAllNodes() bool {
	nodeChecked := func(nodeID NodeID) bool {
		_, ok := r.candidate.votedNodes[nodeID]
		if ok {
			return true
		}
		_, ok = r.candidate.rejectedNodes[nodeID]
		if ok {
			return true
		}
		_, ok = r.candidate.errorNodes[nodeID]
		if ok {
			return true
		}
		return false
	}

	for _, nodeID := range r.storageState.ClusterNodes {
		if !nodeChecked(nodeID) {
			return false
		}
	}
	return true
}

func cloneNodeSet(nodes map[NodeID]struct{}) map[NodeID]struct{} {
	result := map[NodeID]struct{}{}
	for n := range nodes {
		result[n] = struct{}{}
	}
	return result
}

func (r *Raft) candidateSwitchBackToFollowerIfPossible() {
	if !r.candidateCheckedAllNodes() {
		return
	}

	nodes := cloneNodeSet(r.candidate.votedNodes)
	for n := range r.candidate.rejectedNodes {
		nodes[n] = struct{}{}
	}

	if !r.isMajority(nodes) {
		return
	}

	r.state = raftStateFollower
	r.candidate = nil
	r.startFollowerTimer()
}

func (r *Raft) handleVoteResponse(nodeID NodeID, output RequestVoteOutput, err error) {
	if err != nil {
		if r.state == raftStateCandidate {
			r.candidate.errorNodes[nodeID] = struct{}{}
			r.candidateSwitchBackToFollowerIfPossible()

			// still in candidate state
			if r.state == raftStateCandidate {
				r.callRequestVote(nodeID)
			}
		}
		return
	}

	r.checkResponseTerm(output.Term, NullNodeID{})

	if r.state != raftStateCandidate {
		return
	}

	if !output.VoteGranted {
		r.candidate.rejectedNodes[nodeID] = struct{}{}
		r.candidateSwitchBackToFollowerIfPossible()
		return
	}

	r.candidate.votedNodes[nodeID] = struct{}{}
	if !r.isVotedNodesMajority() {
		return
	}

	r.state = raftStateLeader
	r.candidate = nil
	r.leader = &leaderState{
		nodeIndices: map[NodeID]*nodeIndexState{},
	}

	for _, id := range r.storageState.ClusterNodes {
		destNodeID := id
		if id == r.storageState.NodeID {
			continue
		}
		r.callAppendEntries(destNodeID)
	}
}

func (r *Raft) callAppendEntries(nodeID NodeID) {
	indexState := r.leader.nodeIndices[nodeID]
	if indexState == nil {
		indexState = &nodeIndexState{
			nextIndex:  1,
			matchIndex: 0,
		}
		r.leader.nodeIndices[nodeID] = indexState
	}

	if indexState.inProgress {
		return
	}
	indexState.inProgress = true

	r.storage.GetEntries(indexState.nextIndex-1, 128, func(entries []LogEntry) {
		r.client.AppendEntries(AppendEntriesInput{
			NodeID: nodeID,

			Term:     r.storageState.CurrentTerm,
			LeaderID: r.storageState.NodeID,

			PrevLogIndex: entries[0].Index,
			PrevLogTerm:  entries[0].Term,

			Entries: entries[1:],
		}, func(output AppendEntriesOutput, err error) {
			r.handleAppendResponse(indexState, output, err)
		})
	})
}

func (r *Raft) handleAppendResponse(
	indexState *nodeIndexState, output AppendEntriesOutput, err error,
) {
	indexState.inProgress = false
}

func (r *Raft) startFollowerTimer() {
	r.timer.AddTimer(10*time.Second, r.handleTimeout)
}

// Start ...
func (r *Raft) Start() {
	// TODO Check Null
	nullState := r.storage.GetState()

	r.storageState = nullState.State

	r.startFollowerTimer()
}

// RequestVote ...
func (r *Raft) RequestVote(input RequestVoteInput) RequestVoteOutput {
	r.checkResponseTerm(input.Term, NullNodeID{
		Valid:  true,
		NodeID: input.CandidateID,
	})

	granted := false
	if input.Term >= r.storageState.CurrentTerm {
		votedFor := r.storageState.VotedFor
		if votedFor.Valid && votedFor.NodeID == input.CandidateID {
			granted = true
		}
	}

	return RequestVoteOutput{
		Term:        r.storageState.CurrentTerm,
		VoteGranted: granted,
	}
}

// AppendEntriesRPC ...
func (r *Raft) AppendEntriesRPC(input AppendEntriesInput) AppendEntriesOutput {
	return AppendEntriesOutput{}
}

// AppendEntriesInternal ...
func (r *Raft) AppendEntriesInternal(entries []LogEntry) {
	lastEntry := r.storage.GetLastEntry()

	lastIndex := lastEntry.Index
	for i := range entries {
		lastIndex++
		entries[i].Index = lastIndex
	}

	r.storage.AppendEntries(entries, func() {
	})

	for _, id := range r.storageState.ClusterNodes {
		nodeID := id
		if nodeID == r.storageState.NodeID {
			continue
		}
		r.callAppendEntries(nodeID)
	}
}