package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// RaftState captures the state of a Raft node: Follower, Candidate, Leader,
// or Shutdown.
type RaftState uint32
const (
	// Follower is the initial state of a Raft node.
	Follower RaftState = iota

	// Candidate is one of the valid states of a Raft node.
	Candidate

	// Leader is one of the valid states of a Raft node.
	Leader

	// Shutdown is the terminal state of a Raft node.
	Shutdown
)
func (e RaftState) String() string {
	switch e {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

// RPCState captures the state of a RPC contact: Success, Failure, OverTime
type RPCState uint32
const (
	// Success is the initial state of a RPC state.
	Success RPCState = iota
	// Failure  is one of the valid states of a RPC node.
	Failure
	// BACKTRACK is one of the valid states of a RPC node.
	BACKTRACK
	// OverTime is one of the valid states of a RPC node.
	OverTime
)
func (e RPCState) String() string {
	switch e {
	case Success:
		return "Success"
	case Failure:
		return "Failure"
	case OverTime:
		return "OverTime"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

// raftState is used to maintain various state variables
// and provides an interface to set/get the variables in a
// thread safe manner.
type raftState struct {
	// currentTerm commitIndex, lastLogIndex,  must be kept at the top of
	// the struct so they're 64 bit aligned which is a requirement for
	// atomic ops on 32 bit platforms.

	// The current term, cache of StableStore, start from 1
	currentTerm uint64

	// Highest committed log entry
	commitIndex uint64
	// Last applied log to the FSM
	lastApplied uint64

	// protects 4 next fields
	lastLock sync.Mutex

	// Cache the latest snapshot index/term
	lastSnapshotIndex uint64  //lastIncludedIndex
	lastSnapshotTerm  uint64  //lastIncludedTerm

	// Cache the latest log that in Persistent state (stored in FSM)
	lastLogIndex uint64
	lastLogTerm  uint64

	// Tracks running goroutines
	routinesGroup sync.WaitGroup

	// The current state
	state         RaftState

	//dedicated thread calling r.app.apply
	applyLogCh       chan struct{}

	// killCh is used to kill all go routines when state changes
	killCh           chan struct{}
}

// LeaderState leaderState is state that is used while we are a leader.
type LeaderState struct {
	// protects 2 next fields
	indexLock sync.Mutex
	//volatile state on leaders
	nextIndex  []uint64
	matchIndex []uint64

	//information about heartbeat to others
	replState  map[int] *followerReplication

	// stepDown is used to indicate to the leader that we
	// should step down based on information from a follower.
	stepDown   chan struct{}

	commitCh   chan struct{}

	// Tracks replicate goroutines
	routinesGroup sync.WaitGroup

}

func (r *raftState) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&r.commitIndex, index)
}

func (r *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *raftState) setLastApplied(index uint64) {
	atomic.StoreUint64(&r.lastApplied, index)
}

func  (r *raftState) addLastApplied(delta uint64) (new uint64){
	return atomic.AddUint64(&r.lastApplied,delta)
}

func (r *raftState) getLastLog() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastLogIndex
	term = r.lastLogTerm
	r.lastLock.Unlock()
	return
}

//store the Term and Index of latest Log
func (r *raftState) setLastLog(index, term uint64) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLogTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getLastSnapshot() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastSnapshotIndex
	term = r.lastSnapshotTerm
	r.lastLock.Unlock()
	return
}

func (r *raftState) setLastSnapshot(index, term uint64) {
	r.lastLock.Lock()
	r.lastSnapshotIndex = index
	r.lastSnapshotTerm = term
	r.lastLock.Unlock()
}
// Start a goroutine and properly handle the race between a routine
// starting and incrementing, and exiting and decrementing.
func (r *raftState) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

func (r *raftState) waitShutdown() {
	r.routinesGroup.Wait()
}

// getLastIndex returns the last index in stable storage.
// Either from the last log or from the last snapshot.
func (r *raftState) getLastIndex() uint64 {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	return max(r.lastLogIndex, r.lastSnapshotIndex)
}

// getLastEntry returns the last index and term in stable storage.
// Either from the last log or from the last snapshot.
func (r *raftState) getLastEntry() (uint64, uint64) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	if r.lastLogIndex >= r.lastSnapshotIndex {
		return r.lastLogIndex, r.lastLogTerm
	}
	return r.lastSnapshotIndex, r.lastSnapshotTerm
}

func (ls * LeaderState) getState(serverID int) (uint64,uint64)  {
	ls.indexLock.Lock()
	defer ls.indexLock.Unlock()
	return ls.nextIndex[serverID],ls.matchIndex[serverID]
}

func (ls *LeaderState) setNextIndex(serverId int, index uint64)  {
	ls.indexLock.Lock()
	defer ls.indexLock.Unlock()
	if index < 1 {
		ls.nextIndex[serverId] = 1
	}else {
		ls.nextIndex[serverId] = index
	}

}

func (ls *LeaderState) updateStateSuccess(serverID int,match uint64)  {
	ls.indexLock.Lock()
	defer ls.indexLock.Unlock()
	ls.matchIndex[serverID] = match
	ls.nextIndex[serverID] = ls.matchIndex[serverID] + 1
}


