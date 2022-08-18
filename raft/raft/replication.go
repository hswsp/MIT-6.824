package raft

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
// followerReplication is in charge of sending snapshots and log entries from
// this leader during this particular term to a remote follower.
type followerReplication struct {
	Term         uint64
	LeaderId     int32
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Log
	LeaderCommit uint64

	// getLastContact is updated to the current time whenever any response is
	// received from the follower (successful or not). This is used to check
	// whether the leader should step down (Raft.checkLeaderLease()).
	LastContact time.Time
	// LastContactLock protects 'getLastContact'.
	LastContactLock sync.RWMutex

	// stopCh is notified/closed when this leader steps down or the follower is
	// removed from the cluster. In the follower removed case, it carries a log
	// index; replication should be attempted with a best effort up through that
	// index, before exiting.
	stopCh chan uint64

	// StepDown is used to indicate to the leader that we
	// should step down based on information from a follower.
	StepDown chan struct{}

	// triggerCh is notified every time new entries are appended to the log.
	triggerCh chan struct{}
}

func (s *followerReplication) String() string {
	return fmt.Sprintf("Term = %d, LeaderId = %d, PrevLogIndex = %d, PrevLogTerm = %d, LeaderCommit = %d," +
		"Entries  = %s, LastContact = %s",s.Term,s.LeaderId,s.PrevLogIndex,s.PrevLogTerm,s.LeaderCommit,s.Entries,s.LastContact)
}

func (s *followerReplication) getLeaderId() int32 {
	return atomic.LoadInt32(&s.LeaderId)
}

func (s *followerReplication) setLeaderId(peer int32)  {
	atomic.StoreInt32(&s.LeaderId,peer)
}


// getLastContact returns the time of last contact.
func (s *followerReplication) getLastContact() time.Time {
	s.LastContactLock.RLock()
	last := s.LastContact
	s.LastContactLock.RUnlock()
	return last
}

// setLastContact sets the last contact to the current time.
func (s *followerReplication) setLastContact() {
	s.LastContactLock.Lock()
	s.LastContact = time.Now()
	s.LastContactLock.Unlock()
}

// AppendEntries is invoked when we get an append entries RPC call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){

	if rf.killed() {
		reply.Term = 0
		reply.Success = false
		return
	}

	rf.logger.Info("======= got AppendEntries =======","Server ", rf.me,"from leader ", args.LeaderId, ", args: ", args,", current term: ", rf.currentTerm, ", " +
		"current commitIndex: ",rf.commitIndex,", current log: ", rf.logs)

	defer rf.logger.Info("======= finished AppendEntries =======","server ", rf.me," from leader ", args.LeaderId,", args: ", args,", current log: ", rf.logs,
		", reply:", reply)

	currentTerm := rf.getCurrentTerm()
	originLogEntries := rf.getLogEntries()
	originLogEntryLen := uint64(len(originLogEntries))

	reply.ServerID = rf.me
	//Reply false if term < currentTerm (§5.1)
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return
	}

	// Save the current leader
	rf.setLeader(args.LeaderId)

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if args.Term > rf.getCurrentTerm() || rf.getState() != Follower {
		// Ensure transition to follower
		rf.setState(Follower)
		rf.setCurrentTerm(args.Term)
		reply.Term = args.Term
	}

	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if originLogEntryLen < args.PrevLogIndex {
		reply.Term = currentTerm
		reply.Success = false
		// If a follower does not have prevLogIndex in its log
		// it should return with conflictIndex = len(log) + 1 and conflictTerm = None
		reply.ConflictIndex = originLogEntryLen + 1
		reply.ConflictTerm = 0 //represent conflictTerm = None.
		return
	}
	// prevLogTerm default 0 means no LogEntries
	prevLogTerm := uint64(0)
	// PrevLogIndex default 0 means no LogEntries
	if args.PrevLogIndex > 0 {
		prevLogTerm = originLogEntries[args.PrevLogIndex-1].Term
	}

	if args.PrevLogTerm != prevLogTerm {
		reply.Term = currentTerm
		reply.Success = false
		// If a follower does have prevLogIndex in its log, but the term does not match,
		// it should return conflictTerm = log[prevLogIndex - 1].Term
		// then search its log for the first index whose entry has term equal to conflictTerm.
		reply.ConflictTerm = prevLogTerm
		reply.ConflictIndex = getConflictTermIndex(prevLogTerm,originLogEntries)
		return
	}
	// here we have args.PrevLogTerm == rf.logs[args.PrevLogIndex-1].Term && len(rf.logs) >= args.PrevLogIndex

	// Process any new entries

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	argsEntryLen := uint64(len(args.Entries))
	// if args.PrevLogIndex = 0, which means no prev entry, the last new entry index is argsEntryLen
	// otherwise it is args.PrevLogIndex - 1 + argsEntryLen
	preStoredIndex := min(args.PrevLogIndex - 1, 0) // actual index of logs

	// A good example of conflictIndex is 1, because args.Entries[0].Term = rf.logs[args.PrevLogIndex-1].Term
	conflictIndex := uint64(0)
	for conflictIndex < argsEntryLen && (preStoredIndex + conflictIndex < originLogEntryLen) {
		if args.Entries[conflictIndex].Term != originLogEntries[preStoredIndex + conflictIndex].Term {
			break
		}
		conflictIndex++
	}
	rf.logger.Debug("follower starts append logs","preStoredIndex", preStoredIndex,
		"conflictIndex",conflictIndex,"argsEntryLen",argsEntryLen)
	//If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
	//Any elements following the entries sent by the leader MUST be kept.
	//This is because we could be receiving an outdated AppendEntries RPC from the leader
	if conflictIndex < argsEntryLen{
		rf.appendLogEntries(preStoredIndex + conflictIndex,args.Entries[conflictIndex:])
	}

	lastNewEntryIndex := uint64(len(rf.getLogEntries()))

	rf.logger.Info("check Entry commitIndex","args.LeaderCommit",args.LeaderCommit,"CommitIndex",rf.getCommitIndex())
	// Update the commit index
	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.getCommitIndex() {
		rf.setCommitIndex(min(args.LeaderCommit, lastNewEntryIndex))
	}

	// Everything went well, set success
	reply.Term = currentTerm
	reply.Success = true

	rf.persist()
	rf.startApplyLogs()
	//restart your election timer if you get an AppendEntries RPC from the current leader
	rf.setLastContact()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}

	rf.logger.Debug("starting sending an appendEntries request","from ", rf.me," to ", server)

	//Call() is guaranteed to return (perhaps after a delay)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.logger.Debug("sending an appendEntries request returned","from ", rf.me,"to ", server)
	return ok
}

// return LogIndex of the first conflicted entry with conflictTerm
func getConflictTermIndex(conflictTerm uint64,logEntries []Log) uint64 {
	// all the indexes start from 1, so 0 means no conflict
	conflictIndex := uint64(0)
	for i:=0; i<len(logEntries); i++ {
		if logEntries[i].Term == conflictTerm {
			conflictIndex = uint64(i + 1)
			break
		}
	}
	return conflictIndex
}

// find the starting index of conflicted Log Entry, we should not truncate its log!!
func startConflictIndex(argsEntries []Log, prevLogIndex uint64, logEntries []Log) uint64 {
	for i:=uint64(0); i<uint64(len(argsEntries)); i++ {
		if argsEntries[i].Term != logEntries[prevLogIndex - 1 + i].Term {
			return i
		}
	}
	return 0
}

// replicate is a long running routine that replicates log entries to a single
// follower.
func (r *Raft) replicate(id int, s *followerReplication){
	r.logger.Debug("starts an async heartbeating routing ", " leader ",r.me ,"connecting to peer ",id)
	// Start an async heartbeating routing
	stopHeartbeat := make(chan struct{})
	defer func() {
		close(stopHeartbeat)
	}()
	r.leaderState.goFunc(func() { r.heartbeat(id, s, stopHeartbeat) })

	shouldStop := false
	for !shouldStop {
		select {
		case maxIndex :=<-s.stopCh:
			r.logger.Info("removed peer, stopping replication", "peer", id, "last-index", maxIndex)
			// Make a best effort to replicate up to this index
			// 0 means close(s.stopCh)
			if maxIndex > 0 {
				r.updateLastAppended(id,maxIndex,s)
				r.logger.Debug("updated followerReplication of AppendEntries RPCs","peer", id,
					" prevLogIndex", s.PrevLogIndex,"prevLogTerm", s.PrevLogTerm, "LeaderCommit",s.LeaderCommit,"Entries",s.Entries)
			}
			shouldStop = true

		case <-s.triggerCh:
			r.logsLock.RLock()
			lastLogIdx := uint64(len(r.logs))
			r.logsLock.RUnlock()
			r.updateLastAppended(id,lastLogIdx,s)
			r.logger.Debug("updated followerReplication of AppendEntries RPCs","peer", id,
				" prevLogIndex", s.PrevLogIndex,"prevLogTerm", s.PrevLogTerm, "LeaderCommit",s.LeaderCommit,"Entries",s.Entries)

		case <-r.shutdownCh:
			shouldStop = true
			return

		case <-s.StepDown:
			shouldStop = true
			return
		}
	}
}

// heartbeat is used to periodically invoke AppendEntries on a peer
// to ensure they don't time out. This is done async of replicate(),
// since that routine could potentially be blocked on disk IO.
func (r *Raft) heartbeat(id int, s *followerReplication, stopCh chan struct{}){
	shouldStop := false
	var rpcState RPCState
	// repeat during idle periods to prevent election timeouts
	for !shouldStop {
		r.logger.Debug("show current followerReplication date","followerReplication",s)
		// Don't have these loops execute continuously without pausing
		// Wait for the next heartbeat interval or forced notify
		select {
		case <-randomTimeout(r.config().HeartbeatTimeout / 10):
		case <-stopCh:
			return
		}
		args  := &AppendEntriesArgs{
			Term:         s.Term,
			LeaderId:     s.LeaderId,
			PrevLogIndex: s.PrevLogIndex,
			PrevLogTerm:  s.PrevLogTerm,
			Entries:      s.Entries,
			LeaderCommit: s.LeaderCommit,
		}
		shouldStop,rpcState = r.processAppendEntries(id,args)
		if rpcState==Success {
			s.triggerCh <- struct{}{}
			//update heartbeat timer
			s.setLastContact()
		}else if rpcState==Failure{
			r.logger.Warn("appendEntries rejected, sending older logs", "peer", id, "entries", s.Entries)
		}
	}
}

// processAppendEntries send AppendEntriesRPC, return shouldStop(true means converted to follower, heartbeat should stop)
func (r *Raft) processAppendEntries(nodeId int, rpcArg *AppendEntriesArgs) (bool,RPCState) {
	reply := &AppendEntriesReply{}
	ok := r.sendAppendEntries(nodeId, rpcArg, reply)
	if !ok {
		//When sending RPC fails, you should follow the description of the paper and keep retries (&5.3)
		r.logger.Error("failed to heartbeat to", "peer", nodeId)
		// If ok==false, it means that the heartbeat packet is not sent out, there are two possibilities:
		// 1. The leader loses the connection 2. The follower that accepts the heartbeat packet loses the connection
		// If it is possibility 1, all heartbeat packets sent out will be unsuccessful, but will not exit, and will continue to be sent.
		// When connecting again, since the term is definitely smaller than other servers, it will exit the loop and become a Follower
		// If it is possibility 2, it does not affect, continue to send heartbeat packets to other connected servers
		// So, it can be seen that there is no need to do special processing for ok == false
		return false,OverTime
	}
	//you get an AppendEntries RPC from the current leader
	//handle reply
	r.mu.Lock()
	currentTerm :=r.getCurrentTerm()
	args :=r.leaderState.replState[reply.ServerID]
	currentLogs := r.getLogEntries()
	r.mu.Unlock()
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if reply.Term > currentTerm {
		// 退出循环, 转换为follower
		r.logger.Info("turn back to follower due to existing higher term","leader ",r.me,"term: ",reply.Term, "from server ", reply.ServerID )
		r.setState(Follower)
		return true,Failure
	}
	if reply.Success == true {
		//If successful: update nextIndex and matchIndex for follower (§5.3)
		// update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally
		matchIndex := min(args.PrevLogIndex - 1,0) + uint64(len(args.Entries))
		r.logger.Info("update sever state","sever", reply.ServerID," matchIndex ",matchIndex)
		r.leaderState.updateStateSuccess(reply.ServerID,matchIndex)
		//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
		//and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
		copyMatchIndex := make(uint64Slice, len(r.peers))
		copy(copyMatchIndex, r.leaderState.matchIndex)
		copyMatchIndex[r.me] = uint64(len(currentLogs))
		//sort and get the middle to judge the majority
		sort.Slice(copyMatchIndex, copyMatchIndex.Less)
		N := copyMatchIndex[len(r.peers)/2]
		if N > r.getCommitIndex() && currentLogs[N-1].Term == currentTerm {
			r.setCommitIndex(N)
		}
		//check for commitIndex > lastApplied after commitIndex is updated
		r.logger.Info("check for commitIndex > lastApplied","commitIndex",r.getCommitIndex(),"lastApplied",r.getLastApplied())
		r.startApplyLogs()
	}else {
		//If AppendEntries fails because of log inconsistency:
		//decrement nextIndex and retry (§5.3)
		r.logger.Info("start log backtracking","ConflictTerm",reply.ConflictTerm,"current logs",r.getLogEntries())
		// The accelerated log backtracking optimization
		// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
		upperbound, founded := r.lastConfictTermIndex(reply.ConflictTerm)
		if founded {
			// If it finds an entry in its log with ConflictTerm,
			// it should set nextIndex as the one beyond the index of the last entry in that term in its log.
			r.leaderState.setNextIndex(reply.ServerID, upperbound)
		} else {
			r.leaderState.setNextIndex(reply.ServerID, reply.ConflictIndex)
		}
	}

	return false,Success
}

//return the LogIndex just beyond the index of the last entry of the conflictTerm in its log
func (r *Raft) lastConfictTermIndex(conflictTerm uint64) (uint64,bool) {
	entries := r.getLogEntries()
	founded := false
	for i:=0; i<len(entries); i++{
		if entries[i].Term==conflictTerm {
			founded = true
		}
		if entries[i].Term > conflictTerm{
			return uint64(i + 1),founded
		}
	}
	return 0,founded
}

//If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
func (rf *Raft) startApplyLogs() {
	rf.logger.Info("start applying logs,", "leader ",rf.leaderID, "at node ", rf.me,
		" lastApplied: ", rf.getLastIndex(),", commitIndex: ", rf.getCommitIndex())

	// may only be partially submitted
	for lastApplied := rf.getLastIndex(); lastApplied < rf.getCommitIndex(); lastApplied++{
		newLastApplied := lastApplied + 1

		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = int(newLastApplied)

		rf.logsLock.RLock()
		entry := rf.logs[newLastApplied-1]
		msg.Command = entry.Data
		rf.logsLock.RUnlock()

		// Update the last log since it's on disk now
		rf.logger.Debug("check entry index","newLastApplied",newLastApplied,"LastApplied entry",entry)
		rf.setLastLog(entry.Index, entry.Term)

		rf.applyCh <- msg
	}
}

// update follower replication state after a successful AppendEntries RPC.
// it is used to update an AppendEntries RPC request.
func (r *Raft) updateLastAppended(serverID int,lastIndex uint64,s *followerReplication) {
	r.mu.Lock()
	// index of log entry immediately preceding new ones
	// default 0 if no LogEntries
	prevLogIndex := r.leaderState.nextIndex[serverID] - 1
	// term of prevLogIndex entry
	// default 0 if no LogEntries
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 {
		prevLogTerm = r.logs[prevLogIndex - 1].Term
	}
	r.mu.Unlock()

	// Append to the lastIndex.
	// we need to use a consistent value for maxAppendEntries in the lines below in case it ever
	// becomes reloadable.
	r.logger.Debug("Append Log up to the lastIndex","prevLogIndex",prevLogIndex,"lastIndex",lastIndex)
	if prevLogIndex == uint64(len(r.getLogEntries())){
		r.logger.Info("All logs are sent to this peer","peerID",serverID)
	}else{
		entries := append([]Log{}, r.getLogSlices(prevLogIndex - 1, lastIndex)...)//slice needs to be unpacked and appended
		s.Entries = entries
	}

	//update followerReplication
	s.Term = r.getCurrentTerm()
	s.PrevLogIndex = prevLogIndex
	s.PrevLogTerm = prevLogTerm
	s.LeaderCommit = r.getCommitIndex()
}