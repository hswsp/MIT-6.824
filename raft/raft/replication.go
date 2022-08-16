package raft

import (
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

	// commitment tracks the entries acknowledged by followers so that the
	// leader's commit index can advance. It is updated on successful
	// AppendEntries responses.
	commitCh   chan *AppendEntriesReply
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderId     int32
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Log
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	ServerID      int
	Term          uint64
	Success       bool

	// optimization: accelerated log backtracking
	ConflictTerm  uint64
	ConflictIndex uint64
}

func (s *AppendEntriesArgs) getLeaderId() int32 {
	return atomic.LoadInt32(&s.LeaderId)
}

func (s *AppendEntriesArgs) setLeaderId(peer int32)  {
	atomic.StoreInt32(&s.LeaderId,peer)
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

// appendEntries is invoked when we get an append entries RPC call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.logger.Info("got AppendEntries ","Server ", rf.me,"from leader ", args.LeaderId, ", args: ", args,", current term: ", rf.currentTerm, ", " +
		"current commitIndex: ",rf.commitIndex,", current log: ", rf.logs)
	if rf.killed() {
		reply.Term = 0
		reply.Success = false
		return
	}
	defer rf.logger.Info("======= finished AppendEntries ","server ", rf.me," from leader ", args.LeaderId,", args: ", args,", current log: ", rf.logs,
		", reply:", reply,"=======")

	currentTerm := rf.getCurrentTerm()
	originLogEntries := rf.getLogEntries(0)
	logEntryLen := uint64(len(originLogEntries))

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
	if uint64(len(originLogEntries)) < args.PrevLogIndex {
		reply.Term = currentTerm
		reply.Success = false
		//If a follower does not have prevLogIndex in its log
		reply.ConflictIndex = logEntryLen
		reply.ConflictTerm = 0 //represent conflictTerm = None.
		return
	}
	prevLogTerm := uint64(0)
	if args.PrevLogIndex > 0 {
		prevLogTerm = originLogEntries[args.PrevLogIndex-1].Term
	}
	if args.PrevLogTerm != prevLogTerm {
		reply.Term = currentTerm
		reply.Success = false
		//If a follower does have prevLogIndex in its log, but the term does not match,
		//it should return conflictTerm = log[prevLogIndex].Term
		// then search its log for the first index whose entry has term equal to conflictTerm.
		reply.ConflictTerm = prevLogTerm
		reply.ConflictIndex = getConflictTermIndex(prevLogTerm,originLogEntries)
		return
	}

	// Process any new entries
	//If an existing entry conflicts with a new one (same index but different terms),
	//delete the existing entry and all that follow it (§5.3)
	argsEntryLen :=  uint64(len(args.Entries))
	lastNewEntry := args.PrevLogIndex + argsEntryLen
	conflictIndex := uint64(0)
	conflicted := false
	if args.PrevLogIndex + argsEntryLen < logEntryLen {
		conflictIndex, conflicted = startConflictIndex(args.Entries, args.PrevLogIndex,originLogEntries)
	}
	//If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
	//Any elements following the entries sent by the leader MUST be kept.
	if conflicted {
		rf.appendLogEntries(args.PrevLogIndex + conflictIndex,args.Entries[conflictIndex:])
	}

	// Update the commit index
	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.getCommitIndex() {
		rf.setCommitIndex(min(args.LeaderCommit, lastNewEntry))
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

	rf.logger.Debug("finished sending an appendEntries request ","from ", rf.me,"to ", server, "returned")
	return ok
}

func getConflictTermIndex(conflictTerm uint64,logEntries []Log) uint64 {
	conflictIndex := uint64(0) //all the indexes start from 1, so 0 means no conflict
	for i:=0; i<len(logEntries); i++ {
		if logEntries[i].Term == conflictTerm {
			conflictIndex = uint64(i + 1)
			break
		}
	}
	return conflictIndex
}

// false means all matches, we should not truncate its log!!
func startConflictIndex(argsEntries []Log, prevLogIndex uint64, logEntries []Log) (uint64, bool) {
	for i:=uint64(0); i<uint64(len(argsEntries)); i++ {
		if argsEntries[i].Term != logEntries[prevLogIndex + i].Term {
			return i,true
		}
	}
	return 0,false
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
	r.goFunc(func() { r.heartbeat(id, s, stopHeartbeat) })
	shouldStop := false
	for !shouldStop {
		select {
		case lastIndex :=<-s.stopCh:
			r.logger.Info("removed peer, stopping replication", "peer", id, "last-index", lastIndex)
			shouldStop = true
		case reply :=<-s.commitCh:
			//handle reply
			r.mu.Lock()
			currentTerm :=r.getCurrentTerm()
			args :=r.leaderState.replState[reply.ServerID]
			currentLogs := r.getLogEntries(0)
			r.mu.Unlock()
			//If RPC request or response contains term T > currentTerm:
			//set currentTerm = T, convert to follower (§5.1)
			if reply.Term > currentTerm {
				// 退出循环, 转换为follower
				r.logger.Info("turn back to follower due to existing higher term","leader ",r.me,"term: ",reply.Term, "from server ", reply.ServerID )
				r.setState(Follower)
				return
			}
			if reply.Success == true {
				//If successful: update nextIndex and matchIndex for follower (§5.3)
				r.logger.Info("update sever state","sever", reply.ServerID," matchIndex ",args.PrevLogIndex + uint64(len(args.Entries)))
				r.leaderState.updateStateSuccess(reply.ServerID,args.PrevLogIndex + uint64(len(args.Entries)))
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
				r.startApplyLogs()
			}else {
				//If AppendEntries fails because of log inconsistency:
				//decrement nextIndex and retry (§5.3)

				//Upon receiving a conflict response, the leader should first search its log for conflictTerm.
				upperbound, founded := r.lastConfictTermIndex(reply.ConflictTerm)
				if founded {
					//If it finds an entry in its log with that term,
					//it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
					r.leaderState.setNextIndex(reply.ServerID, upperbound)
				} else {
					r.leaderState.setNextIndex(reply.ServerID, reply.ConflictIndex)
				}
			}
		case <-r.shutdownCh:
			shouldStop = true
			return
		}
	}
}

// heartbeat is used to periodically invoke AppendEntries on a peer
// to ensure they don't time out. This is done async of replicate(),
// since that routine could potentially be blocked on disk IO.
func (r *Raft) heartbeat(id int, s *followerReplication, stopCh chan struct{}){
	// repeat during idle periods to prevent election timeouts
	for {
		// Don't have these loops execute continuously without pausing
		// Wait for the next heartbeat interval or forced notify
		select {
		case <-randomTimeout(r.config().HeartbeatTimeout / 5):
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
		reply := &AppendEntriesReply{}
		ok := r.sendAppendEntries(id, args, reply)
		if !ok {
			//When sending RPC fails, you should follow the description of the paper and keep retries (&5.3)
			r.logger.Error("failed to heartbeat to", "peer", id)
			// If ok==false, it means that the heartbeat packet is not sent out, there are two possibilities:
			// 1. The leader loses the connection 2. The follower that accepts the heartbeat packet loses the connection
			// If it is possibility 1, all heartbeat packets sent out will be unsuccessful, but will not exit, and will continue to be sent.
			// When connecting again, since the term is definitely smaller than other servers, it will exit the loop and become a Follower
			// If it is possibility 2, it does not affect, continue to send heartbeat packets to other connected servers
			// So, it can be seen that there is no need to do special processing for ok == false
		}else{
			//you get an AppendEntries RPC from the current leader
			s.commitCh <- reply
			//update heartbeat timer
			s.setLastContact()
		}
	}
}
