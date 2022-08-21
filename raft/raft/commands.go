package raft

import (
	"fmt"
	"sync/atomic"
)

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
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term uint64
	VoteGranted bool
	VoterID string
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


func (arg AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term = %d, LeaderId = %d, PrevLogIndex = %d, PrevLogTerm = %d, LeaderCommit = %d, Entries = %s",
		arg.Term,arg.LeaderId,arg.PrevLogIndex,arg.PrevLogTerm,arg.LeaderCommit,arg.Entries)
}

func (s *AppendEntriesArgs) getLeaderId() int32 {
	return atomic.LoadInt32(&s.LeaderId)
}

func (s *AppendEntriesArgs) setLeaderId(peer int32)  {
	atomic.StoreInt32(&s.LeaderId,peer)
}
