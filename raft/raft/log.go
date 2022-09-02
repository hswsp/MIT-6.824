/*
 * @Author: hswsp hswsp@mail.ustc.edu.cn
 * @Date: 2022-07-31 17:51:11
 * @LastEditors: hswsp hswsp@mail.ustc.edu.cn
 * @LastEditTime: 2022-08-03 16:01:10
 * @FilePath: /src/raft/log.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package raft

import "fmt"

// LogType describes various types of log entries.
type LogType uint8
const (
	// LogCommand is applied to a user FSM.
	LogCommand LogType = iota

	// LogNoop is used to assert leadership.
	LogNoop

	// LogAddPeerDeprecated is used to add a new peer. This should only be used with
	// older protocol versions designed to be compatible with unversioned
	// Raft servers. See comments in config.go for details.
	LogAddPeerDeprecated

	// LogRemovePeerDeprecated is used to remove an existing peer. This should only be
	// used with older protocol versions designed to be compatible with
	// unversioned Raft servers. See comments in config.go for details.
	LogRemovePeerDeprecated

	// LogBarrier is used to ensure all preceding operations have been
	// applied to the FSM. It is similar to LogNoop, but instead of returning
	// once committed, it only returns once the FSM manager acks it. Otherwise,
	// it is possible there are operations committed but not yet applied to
	// the FSM.
	LogBarrier

	// LogConfiguration establishes a membership change configuration. It is
	// created when a server is added, removed, promoted, etc. Only used
	// when protocol version 1 or greater is in use.
	LogConfiguration
)

// Log entries are replicated to all members of the Raft cluster
// and form the heart of the replicated state machine.
type Log struct {
	// Index holds the index of the log entry. Start from 1. We use log[Index-1] to fetch
	Index uint64

	// Term holds the election term of the log entry. Start from 1.
	Term uint64

	// Type holds the type of the log entry.
	Type LogType

	// Data holds the log entry's type-specific data.
	Data interface{}

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate. This value is a part of
	// the log, so very large values could cause timing issues.
	//
	// N.B. It is _up to the client_ to handle upgrade paths. For instance if
	// using this with go-raftchunking, the client should ensure that all Raft
	// peers are using a version that can handle that extension before ever
	// actually triggering chunking behavior. It is sometimes sufficient to
	// ensure that non-leaders are upgraded first, then the current leader is
	// upgraded, but a leader changeover during this process could lead to
	// trouble, so gating extension behavior via some flag in the client
	// program is also a good idea.
	Extensions []byte
}

func (e Log) String() string {
	return fmt.Sprintf("Index = %d, Term = %d, Type = %d, Data = %s", e.Index,e.Term,e.Type,e.Data)
}

// return slice of current logs
func (r *Raft) getLogEntries() []Log{
	r.logsLock.RLock()
	entries :=r.logs
	r.logsLock.RUnlock()
	return entries
}

// return [startPos, endIndex) in logs, endIndex not included
func (r *Raft) getLogSlices(startPos uint64,endIndex uint64) []Log{
	r.logsLock.RLock()
	entries :=r.logs[startPos:endIndex]
	r.logsLock.RUnlock()
	return entries
}

// cut logs before cutPos
func (r *Raft) cutLogEntries(cutPos uint64)  {
	r.logsLock.Lock()
	defer r.logsLock.Unlock()
	// copy on write
	sLogs := make([]Log, 0)
	copy(sLogs,r.logs[cutPos :])
	r.logs = sLogs
}

//cut to logEntries[0:cutPos) and then append new logSlice
func (r *Raft) appendLogEntries(cutPos uint64, logSlice []Log) {
	r.logsLock.Lock()
	defer r.logsLock.Unlock()
	r.logs = append(r.logs[:cutPos], logSlice...)
	lastEntry := r.logs[len(r.logs)-1]
	r.setLastLog(lastEntry.Index,lastEntry.Term)
}

// fetch log entry by index
func (r *Raft)getEntryByOffset(index uint64) Log{
	r.lastLock.Lock()
	base := r.lastSnapshotIndex
	r.lastLock.Unlock()

	r.logsLock.RLock()
	entry := r.logs[index - base]
	r.logsLock.RUnlock()
	return entry
}

//get log term by index after snapshot
func (rf *Raft) getLogTermByIndex(index uint64) uint64 {
	rf.lastLock.Lock()
	var offset int64
	offset = int64(index)-int64(1 + rf.lastSnapshotIndex)  // may overflow here, caution!!!!
	if offset < 0 {
		return rf.lastSnapshotTerm
	}
	rf.lastLock.Unlock()

	rf.logsLock.Lock()
	defer rf.logsLock.Unlock()
	return rf.logs[offset].Term
}
