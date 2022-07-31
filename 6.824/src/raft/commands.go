package raft

import "6.824/labrpc"

// AppendEntriesRequest is the command used to append entries to the
// replicated log.
type AppendEntriesRequest struct {

	// Provide the current term and leader
	Term uint64

	// RPC end points of leader
	Leader    *labrpc.ClientEnd

	// Provide the previous entries for integrity checking
	PrevLogIndex uint64
	PrevLogTerm  uint64

	// New entries to commit
	Entries []*Log

	// Commit index on the leader
	LeaderCommitIndex uint64
}