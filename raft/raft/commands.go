package raft

import (
	"fmt"
	"sync/atomic"
	"time"
)

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term uint64
	CandidateId int32
	// Cache the latest log from LogStore
	LastLogIndex uint64
	LastLogTerm  uint64

	// for Debug
	Time time.Time
}
func (arg RequestVoteArgs) String() string {
	return fmt.Sprintf("Term = %d, CandidateId = %v, LastLogIndex = %v, LastLogTerm = %v, request Time = %v",
		arg.Term,arg.CandidateId,arg.LastLogIndex,arg.LastLogTerm, arg.Time)
}
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term uint64
	VoteGranted bool
	VoterID uint64

	// for Debug
	Time time.Time
}
func (arg RequestVoteReply) String() string {
	return fmt.Sprintf("Term = %d, VoteGranted = %v, VoterID = %v, reply time = %v",
		arg.Term,arg.VoteGranted,arg.VoterID, arg.Time)
}

// AppendEntriesArgs is the command used to append entries to the
// replicated log.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderId     int32
	PrevLogIndex uint64 //index of log entry immediately preceding new ones
	PrevLogTerm  uint64 //term of prevLogIndex entry
	Entries      []Log
	LeaderCommit uint64
}

func (arg AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term = %d, LeaderId = %d, PrevLogIndex = %d, PrevLogTerm = %d, LeaderCommit = %d, Entries = %s",
		arg.Term,arg.LeaderId,arg.PrevLogIndex,arg.PrevLogTerm,arg.LeaderCommit,arg.Entries)
}


type AppendEntriesReply struct {
	ServerID      int
	Term          uint64
	Success       bool

	// optimization: accelerated log backtracking
	ConflictTerm  uint64  // first Log Term that conflicts between follower and leader
	ConflictIndex uint64  // first Log Index that conflicts between follower and leader
}

func (arg AppendEntriesReply) String() string {
	return fmt.Sprintf("ServerID = %d, Term = %d, Success = %v, ConflictTerm = %d, ConflictIndex = %d",
		arg.ServerID,arg.Term,arg.Success,arg.ConflictTerm,arg.ConflictIndex)
}

// InstallSnapshotRequest is the command sent to a Raft peer to bootstrap its
// log (and state machine) from a snapshot on another peer.
type InstallSnapshotRequest struct {
	Term        uint64
	LeaderId    int32    // LeaderId of request

	// These are the last index/term included in the snapshot
	LastLogIndex uint64
	LastLogTerm  uint64

	// Raw byte stream data of snapshot
	Data         []byte

	// Size of the snapshot
	Size         int64
}

func (arg InstallSnapshotRequest) String() string {
	return fmt.Sprintf("Term = %d, LeaderId = %d, LastLogIndex = %d, LastLogTerm = %d, Data = %v, Size = %d",
		arg.Term,arg.LeaderId,arg.LastLogIndex,arg.LastLogTerm,arg.Data,arg.Size)
}
// InstallSnapshotReply is the response returned from an
// InstallSnapshotRequest.
type InstallSnapshotReply struct {
	Term    uint64
	Success bool
}
func (arg InstallSnapshotReply) String() string {
	return fmt.Sprintf("Term = %d, Success = %v",
		arg.Term,arg.Success)
}

func (s *AppendEntriesArgs) getLeaderId() int32 {
	return atomic.LoadInt32(&s.LeaderId)
}

func (s *AppendEntriesArgs) setLeaderId(peer int32)  {
	atomic.StoreInt32(&s.LeaderId,peer)
}
