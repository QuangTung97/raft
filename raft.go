package raft

import (
	"math"
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

	state raftState

	follower  *followerState
	candidate *candidateState
	leader    *leaderState

	storageState StorageState
}

type followerState struct {
	timerCancel TimerCancelFunc
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

func (r *Raft) handleFollowerTimeout() {
	r.storageState.CurrentTerm++
	r.storageState.VotedFor = NullNodeID{
		Valid:  true,
		NodeID: r.storageState.NodeID,
	}

	r.storage.PutState(r.storageState)

	r.becomeCandidate()

	// do request votes
	for _, id := range r.storageState.ClusterNodes {
		nodeID := id
		if nodeID == r.storageState.NodeID {
			continue
		}
		r.callRequestVote(nodeID)
	}
}

type stateSnapshot struct {
	r     *Raft
	state raftState
	term  TermNumber
}

func (r *Raft) getStateSnapshot() stateSnapshot {
	return stateSnapshot{
		r:     r,
		state: r.state,
		term:  r.storageState.CurrentTerm,
	}
}

func (s stateSnapshot) isValid() bool {
	raft := s.r
	if s.state == raft.state && s.term == raft.storageState.CurrentTerm {
		return true
	}
	return false
}

func (r *Raft) callRequestVote(nodeID NodeID) {
	ss := r.getStateSnapshot()

	r.client.RequestVote(RequestVoteInput{
		NodeID:      nodeID,
		Term:        r.storageState.CurrentTerm,
		CandidateID: r.storageState.NodeID,
	}, func(output RequestVoteOutput, err error) {
		if err == nil {
			r.checkTerm(output.Term)
		}
		if !ss.isValid() {
			return
		}
		r.handleVoteResponse(nodeID, output, err)
	})
}

func (r *Raft) checkTerm(term TermNumber) {
	if term > r.storageState.CurrentTerm {
		r.storageState.CurrentTerm = term
		r.storageState.VotedFor = NullNodeID{}

		r.storage.PutState(r.storageState)

		r.becomeFollower()
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

	r.becomeFollower()
}

func (r *Raft) handleVoteResponse(nodeID NodeID, output RequestVoteOutput, err error) {
	if err != nil {
		r.candidate.errorNodes[nodeID] = struct{}{}
		r.candidateSwitchBackToFollowerIfPossible()

		// still in candidate state
		if r.state == raftStateCandidate {
			r.callRequestVote(nodeID)
		}
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

	r.becomeLeader()

	for _, id := range r.storageState.ClusterNodes {
		destNodeID := id
		if id == r.storageState.NodeID {
			continue
		}
		r.callAppendEntries(destNodeID)
	}
}

func (r *Raft) getIndexState(nodeID NodeID) *nodeIndexState {
	indexState := r.leader.nodeIndices[nodeID]
	if indexState == nil {
		indexState = &nodeIndexState{
			nextIndex:  1,
			matchIndex: 0,
		}
		r.leader.nodeIndices[nodeID] = indexState
	}
	return indexState
}

func (r *Raft) callAppendEntries(nodeID NodeID) {
	indexState := r.getIndexState(nodeID)
	if indexState.inProgress {
		return
	}
	indexState.inProgress = true

	ss := r.getStateSnapshot()

	r.storage.GetEntries(indexState.nextIndex-1, 128, func(entries []LogEntry) {
		if !ss.isValid() {
			return // TODO
		}

		newSS := r.getStateSnapshot()
		lastEntry := entries[len(entries)-1]

		r.client.AppendEntries(AppendEntriesInput{
			NodeID: nodeID,

			Term:     r.storageState.CurrentTerm,
			LeaderID: r.storageState.NodeID,

			PrevLogIndex: entries[0].Index,
			PrevLogTerm:  entries[0].Term,

			Entries: entries[1:],
		}, func(output AppendEntriesOutput, err error) {
			// TODO Check Term

			// TODO Check Error

			if !newSS.isValid() {
				// TODO
			}
			if !output.Success {
				// TODO
			}
			r.handleAppendResponse(indexState, output, err, lastEntry.Index)
		})
	})
}

func (r *Raft) handleAppendResponse(
	indexState *nodeIndexState, output AppendEntriesOutput, err error, lastIndex LogIndex,
) {
	r.increaseMatchIndex(indexState, lastIndex)
}

func (r *Raft) increaseMatchIndex(indexState *nodeIndexState, lastIndex LogIndex) {
	indexState.inProgress = false
	if lastIndex > indexState.matchIndex {
		indexState.matchIndex = lastIndex
		r.increaseCommitIndex()
	}
}

func (r *Raft) assertTrue(b bool) {
	if !b {
		panic("invalid assertion")
	}
}

func (r *Raft) increaseCommitIndex() {
	r.assertTrue(len(r.leader.nodeIndices) > 0)

	lastCommitIndex := r.storage.GetCommitIndex()
	minIndex := LogIndex(math.MaxUint64)
	greaterCount := 0

	for _, indexState := range r.leader.nodeIndices {
		if indexState.matchIndex > lastCommitIndex {
			greaterCount++
			if indexState.matchIndex < minIndex {
				minIndex = indexState.matchIndex
			}
		}
	}

	majority := len(r.storageState.ClusterNodes)/2 + 1
	if greaterCount >= majority {
		r.storage.SetCommitIndex(minIndex)
	}
}

func (r *Raft) becomeFollower() {
	if r.state == raftStateFollower {
		return
	}
	r.state = raftStateFollower
	r.candidate = nil
	r.leader = nil
	r.follower = &followerState{}
	r.addFollowerTimer()
}

func (r *Raft) becomeCandidate() {
	r.follower.timerCancel()

	r.state = raftStateCandidate
	r.candidate = &candidateState{
		votedNodes: map[NodeID]struct{}{
			r.storageState.NodeID: {},
		},
		rejectedNodes: map[NodeID]struct{}{},
		errorNodes:    map[NodeID]struct{}{},
	}

	r.leader = nil
	r.follower = nil
}

func (r *Raft) becomeLeader() {
	r.state = raftStateLeader
	r.candidate = nil
	r.follower = nil
	r.leader = &leaderState{
		nodeIndices: map[NodeID]*nodeIndexState{},
	}
}

// Start ...
func (r *Raft) Start() {
	// TODO Check Null
	nullState := r.storage.GetState()

	r.storageState = nullState.State

	r.state = 0
	r.becomeFollower()
}

// RequestVote ...
func (r *Raft) RequestVote(input RequestVoteInput) RequestVoteOutput {
	if input.Term > r.storageState.CurrentTerm {
		r.storageState.CurrentTerm = input.Term
		r.storageState.VotedFor = NullNodeID{
			Valid:  true,
			NodeID: input.CandidateID,
		}

		r.storage.PutState(r.storageState)

		r.becomeFollower()
	}

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

func (r *Raft) addFollowerTimer() {
	ss := r.getStateSnapshot()
	r.follower.timerCancel = r.timer.AddTimer(10*time.Second, func() {
		if !ss.isValid() {
			return
		}
		r.handleFollowerTimeout()
	})
}

// AppendEntriesRPC ...
func (r *Raft) AppendEntriesRPC(input AppendEntriesInput) AppendEntriesOutput {
	if input.Term < r.storageState.CurrentTerm {
		return AppendEntriesOutput{
			Term:    r.storageState.CurrentTerm,
			Success: false,
		}
	}

	r.checkTerm(input.Term)

	if r.state == raftStateCandidate {
		r.becomeFollower()
	} else {
		r.follower.timerCancel()
		r.addFollowerTimer()
	}

	if len(input.Entries) > 0 {
		r.storage.AppendEntries(input.Entries, true, nil)
	}

	return AppendEntriesOutput{
		Term:    r.storageState.CurrentTerm,
		Success: true,
	}
}

func (r *Raft) storageAppendEntriesAsync(entries []LogEntry) {
	state := r.getIndexState(r.storageState.NodeID)
	if state.inProgress {
		return // TODO
	}
	state.inProgress = true

	lastIndex := entries[len(entries)-1].Index

	ss := r.getStateSnapshot()

	r.storage.AppendEntries(entries, false, func() {
		if !ss.isValid() {
			// TODO
		}
		state.inProgress = false
		r.increaseMatchIndex(state, lastIndex)
	})
}

// AppendEntriesInternal from the state machine
func (r *Raft) AppendEntriesInternal(entries []LogEntry) {
	lastEntry := r.storage.GetLastEntry()

	lastIndex := lastEntry.Index
	for i := range entries {
		lastIndex++
		entries[i].Index = lastIndex
	}

	r.storageAppendEntriesAsync(entries)

	for _, id := range r.storageState.ClusterNodes {
		nodeID := id
		if nodeID == r.storageState.NodeID {
			continue
		}
		r.callAppendEntries(nodeID)
	}
}
