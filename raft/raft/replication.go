package raft

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

// followerReplication is in charge of sending snapshots and log entries from
// this leader during this particular term to a remote follower.
type followerReplication struct {
	Term         uint64
	LeaderId     int32
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64

	Entries      []Log
	EntriesLock sync.RWMutex

	// getLastContact is updated to the current time whenever any response is
	// received from the follower (successful or not). This is used to check
	// whether the leader should step down (Raft.checkLeaderLease()).
	LastContact time.Time
	// LastContactLock protects 'getLastContact'.
	LastContactLock sync.RWMutex

	// failures counts the number of failed RPCs since the last success, which is
	// used to apply backoff.
	Failures uint64

	// stopCh is notified/closed when this leader steps down or the follower is
	// removed from the cluster. In the follower removed case, it carries a log
	// index; replication should be attempted with a best effort up through that
	// index, before exiting.
	stopCh chan uint64

	// stepDown is used to indicate to the leader that we
	// should step down based on information from a follower.
	stepDown chan struct{}

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

func (s *followerReplication) drainTriggerCh() {
	select {
	case <-s.triggerCh:
		fmt.Printf("drain out last trigger, time = %v...\n",time.Now())
	default:
		return
	}
}

// AppendEntries is invoked when we get an append entries RPC call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){

	rf.logger.Info("======= got AppendEntries =======","Server ", rf.me,"from leader ", args.LeaderId, ", args: ", args,", current term: ", rf.currentTerm, ", " +
		"current commitIndex: ",rf.commitIndex,", current log: ", rf.getLogEntries())

	if rf.killed() {
		reply.Term = 0
		reply.Success = false
		return
	}

	currentTerm := rf.getCurrentTerm()
	originLogEntries := rf.getLogEntries()
	// we fetch them in a same lock.
	rf.lastLock.Lock()
	lastLogIndex := rf.lastLogIndex
	lastSnapshotIndex :=rf.lastSnapshotIndex
	if lastLogIndex < lastSnapshotIndex{
		lastLogIndex = lastSnapshotIndex
	}
	rf.lastLock.Unlock()

	reply.ServerID = rf.me
	// Reply false if term < currentTerm (§5.1)
	// out of date RPC
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return
	}

	// Save the current leader
	rf.setLeader(args.LeaderId)

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if args.Term > currentTerm || rf.getState() != Follower {
		// Ensure transition to follower
		rf.setState(Follower)
		rf.setCurrentTerm(args.Term)
		reply.Term = args.Term
	}

	rf.logger.Info("check index","PrevLogIndex",args.PrevLogIndex,"lastLogIndex",lastSnapshotIndex,
		"lastSnapshotIndex",lastSnapshotIndex)
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if lastSnapshotIndex > args.PrevLogIndex || lastLogIndex < args.PrevLogIndex {
		reply.Term = currentTerm
		reply.Success = false
		// If a follower does not have prevLogIndex in its log
		// it should return with conflictIndex = lastLogIndex + 1 and conflictTerm = None
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = 0 //represent conflictTerm = None.

		rf.persist()
		return
	}
	// prevLogTerm default 0 means no LogEntries
	prevLogTerm := uint64(0)
	// PrevLogIndex default 0 means no LogEntries
	if args.PrevLogIndex > 0 {
		// prevLogTerm = originLogEntries[args.PrevLogIndex-1].Term
		prevLogTerm = rf.getLogTermByIndex(args.PrevLogIndex)
	}

	if args.PrevLogTerm != prevLogTerm {
		reply.Term = currentTerm
		reply.Success = false
		// If a follower does have prevLogIndex in its log, but the term does not match,
		// it should return conflictTerm = log[prevLogIndex - 1].Term
		// then search its log for the first index whose entry has term equal to conflictTerm.
		reply.ConflictTerm = prevLogTerm
		reply.ConflictIndex = getConflictTermIndex(prevLogTerm,originLogEntries)

		rf.persist()
		return
	}
	// here we have args.PrevLogTerm == rf.logs[args.PrevLogIndex-1].Term && len(rf.logs) >= args.PrevLogIndex

	// Process any new entries

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	argsEntryLen := int64(len(args.Entries))
	originLogEntryLen := int64(len(originLogEntries))

	// if args.PrevLogIndex = 0, which means no prev entry, the last new entry index is argsEntryLen
	// otherwise it is args.PrevLogIndex - 1 + argsEntryLen
	// actual index of start of new Entries, should be Entries[0].Index - 1
	newEntriesStartOffset := int64(args.PrevLogIndex) - int64(lastSnapshotIndex) // convert to offset
	if newEntriesStartOffset < 0 {
		newEntriesStartOffset = 0
	}

	// A good example of conflictOffset is 1, because args.Entries[0].Term = rf.logs[args.PrevLogIndex-1].Term
	conflictOffset := int64(0)
	for conflictOffset < argsEntryLen && (newEntriesStartOffset+ conflictOffset < originLogEntryLen) {
		if args.Entries[conflictOffset].Term != originLogEntries[newEntriesStartOffset+ conflictOffset].Term {
			break
		}
		conflictOffset++
	}
	rf.logger.Debug("follower check appending logs","newEntriesStartOffset", newEntriesStartOffset,
		"conflictOffset",conflictOffset,"argsEntryLen",argsEntryLen)

	// If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
	// Any elements following the entries sent by the leader MUST be kept.
	// This is because we could be receiving an outdated AppendEntries RPC from the leader
	if conflictOffset < argsEntryLen{
		rf.logger.Debug("follower start appending logs","peer",rf.me," entries ",args.Entries)
		newEntriesCutIndex := uint64(newEntriesStartOffset + conflictOffset) + 1 + lastSnapshotIndex
		rf.appendLogEntries(newEntriesCutIndex,args.Entries[conflictOffset:])
	}

	//update the logs
	lastNewEntryIndex,_ := rf.getLastLog()

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
	//apply to FSM
	rf.applyLogCh <- struct{}{}
	//restart your election timer if you get an AppendEntries RPC from the current leader
	rf.setLastContact()

	rf.logger.Info("======= finished AppendEntries =======","server ", rf.me," from leader ", args.LeaderId,", args: ", args,", current log: ", rf.getLogEntries(),
		", reply:", reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}

	rf.logger.Debug("starting sending an appendEntries request","from ", rf.me," to ", server)

	//Call() is guaranteed to return (perhaps after a delay)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.logger.Debug("sending an appendEntries request returned","from ", rf.me,"to ", server,
		" AppendEntriesArgs",args," AppendEntriesReply",reply," ok",ok)
	return ok
}

// return LogIndex of the first conflicted entry with conflictTerm
func getConflictTermIndex(conflictTerm uint64,logEntries []Log) uint64 {
	// all the indexes start from 1, so 0 means no conflict
	conflictIndex := uint64(0)
	for i:=0; i<len(logEntries); i++ {
		if logEntries[i].Term == conflictTerm {
			//conflictIndex = uint64(i + 1)
			// conflictIndex is the actual index of the log !
			conflictIndex = logEntries[i].Index
			break
		}
	}
	return conflictIndex
}

// replicate is a long running routine that replicates log entries to a single
// follower.
func (r *Raft) replicate(id int, s *followerReplication){
	r.logger.Debug("starts an async heartbeating routing ", " leader ",r.me ,"connecting to peer ",id)
	// Start an async heartbeating routing
	stopHeartbeat := make(chan struct{})
	defer func() {
		r.logger.Info("stops an async heartbeating routing ", " leader ",r.me ,"connecting to peer ",id)
		close(stopHeartbeat)
		// do not forget to close the channel!!!!!!!
		// otherwise the main thread will be blocked on repl.triggerCh <- struct{}{} !!!!
		close(s.stopCh)
	}()
	r.goFunc(func() { r.heartbeat(id, s, stopHeartbeat) })

	//send our logs to follower first
	shouldStop := false
	for !shouldStop {
		select {
		case <-s.stepDown:
			shouldStop = true
		case maxIndex :=<-s.stopCh:
			r.logger.Info("removed peer, stopping replication", "peer", id, "last-index", maxIndex)
			// Make a best effort to replicate up to this index
			// 0 means close(s.stopCh)
			if maxIndex > 0 {
				r.replicateTo(id,maxIndex,s)
			}
			shouldStop = true

		case <-s.triggerCh:
			r.logger.Info("new logs added info has propagated to followers","peer",id)
			shouldStop = r.replicateTo(id,r.getLastIndex(),s)

		case <-r.shutdownCh:
			r.logger.Warn("replicate loop shut down!!","peer",r.me)
			shouldStop = true

		//// we will dismiss s.triggerCh notification
		//// when we try to send old logs due to network failure,
		//// so we need this backoff
		//case <-randomTimeout(r.config().CommitTimeout):
		//	// This is _not_ our heartbeat mechanism but is to ensure
		//	// followers quickly learn the leader's commit index when
		//	// raft commits stop flowing naturally. The actual heartbeats
		//	// can't do this to keep them unblocked by disk IO on the
		//	// follower. See https://github.com/hashicorp/raft/issues/282.
		//	lastLogIdx, _ := r.getLastLog()
		//	shouldStop = r.replicateTo(id,lastLogIdx,s)
		}
	}
}

func (r *Raft) replicateTo(serverID int,lastIndex uint64,s *followerReplication) bool {
	shouldStop := false
BACKTRACK:
	if r.getState() != Leader{
		r.logger.Warn("updateLastAppended shut down!!","peer",r.me)
		return true
	}
	if s.Term != r.getCurrentTerm(){
		r.logger.Warn("current term has changed, no longer be the leader, shut down!!",
			"peer",r.me,"current term",r.getCurrentTerm()," Replication RPC term",s.Term)
		return true
	}

	// InstallSnapShot，The follower's log is smaller than leader's snapshot state,
	// send snapshot to the peer
	nextIndex,_ := r.leaderState.getState(serverID)
	lastIncludeIndex,_ :=r.getLastSnapshot()
	if nextIndex < lastIncludeIndex{
		r.logger.Info("InstallSnapShot to followers","leader",r.me," peer",serverID,
			"current term",r.getCurrentTerm()," Replication RPC term",s.Term)
		r.goFunc(func() {r.leaderSendSnapShot(serverID)})
		return shouldStop
	}

	// each command is sent to each peer just once.
	args  := r.updateLastAppended(serverID,lastIndex,s)
	r.logger.Debug("updated followerReplication of AppendEntries RPCs","peer", serverID,
		" followerReplication ",s)
	// we should copy the origin entries, and do not use the address of s.Entries
	// otherwise it may be changed during RPC!!!!
	// PrevLogIndex and Entries should set at the same time to ensure match!!!
	//args  := &AppendEntriesArgs{
	//	Term:         s.Term,
	//	LeaderId:     s.LeaderId,
	//	PrevLogIndex: s.PrevLogIndex,
	//	PrevLogTerm:  s.PrevLogTerm,
	//	Entries:      s.Entries,
	//	LeaderCommit: s.LeaderCommit,
	//}

	// Don't have these loops execute continuously without pausing,
	// since that will slow your implementation enough that it fails tests
//START:
	if s.Failures> 0 {
		r.logger.Warn("appendEntries cannot connect to peers after retries", "peer", serverID, "entries", s.Entries," s.Failures ", s.Failures)
		select {
		case <-time.After(backoff(failureWait, s.Failures, maxFailureScale)):
		// if we are here to wait , we will miss the last best effort signal
		case maxIndex :=<-s.stopCh:
			if maxIndex > 0 {
				lastIndex = maxIndex
				goto BACKTRACK
			}
			// last time to send RPC, it should stop after finish
			shouldStop  = true
		case <-r.shutdownCh:
			r.logger.Warn("replicateTo loop shut down!!","peer",r.me)
			return true
		case <-s.stepDown:
			return true
		}
	}

	r.logger.Debug("replicateTo show current followerReplication data",
		"to peer",serverID, " followerReplication",s," args",args)

	rpcState := r.processAppendEntries(serverID,args)
	if rpcState==Success {
		// update heartbeat timer
		s.setLastContact()
		r.logger.Debug("replicateTo AppendEntries RPCs success","peer", serverID, " last contact",s.getLastContact())
		s.Entries = []Log{}
		s.Failures = 0
	}else if rpcState==OverTime{
		// Don't have these loops execute continuously without pausing
		s.Failures ++
		r.logger.Warn("replicateTo AppendEntries cannot connect to peers", "peer", serverID, "entries", s.Entries, "s.Failures ", s.Failures)
	}else if rpcState == Failure{
		// Check for a newer term, stop running
		r.logger.Warn("replicateTo AppendEntries rejected, sending older logs", "peer", serverID, "entries", s.Entries)
		s.Failures = 0 //s.Failures only count for OverTime, we should clear in any other condition
		//s.stepDown <- struct{}{}
		shouldStop =  true
	}else{
		r.logger.Warn("replicateTo AppendEntries has log conflict, need log back tracking", "peer", serverID, "entries", s.Entries)
		s.Failures = 0
		goto BACKTRACK
	}
	return shouldStop
}

// update follower replication state after a successful AppendEntries RPC.
// it is used to update an AppendEntries RPC request.
func (r *Raft) updateLastAppended(serverID int,lastIndex uint64,s *followerReplication) *AppendEntriesArgs{
	// PrevLogIndex and Entries should set at the same time to ensure match!!!
	s.EntriesLock.Lock()
	defer s.EntriesLock.Unlock()

	r.updateHeartbeatInfo(serverID,s)
	prevLogIndex :=s.PrevLogIndex
	// Append to the lastIndex.
	// we need to use a consistent value for maxAppendEntries in the lines below in case it ever
	// becomes reloadable.
	r.logger.Debug("Append Log up to the lastIndex","peer", serverID, " prevLogIndex",prevLogIndex," lastIndex",lastIndex)
	if prevLogIndex >= lastIndex {
		r.logger.Info("All logs are sent to this peer, send empty entries","peerID",serverID)
		s.Entries = []Log{}
	}else{
		//If last log index ≥ nextIndex (nextIndex - 1 < last log index) for a follower:
		//send AppendEntries RPC with log entries starting at nextIndex
		// we do not need to send logs[ prevLogIndex - 1] actually
		entries := append([]Log{}, r.getLogSlices(prevLogIndex, lastIndex)...)//slice needs to be unpacked and appended
		s.Entries = entries
	}
	return &AppendEntriesArgs{
		Term:         s.Term,
		LeaderId:     s.LeaderId,
		PrevLogIndex: s.PrevLogIndex,
		PrevLogTerm:  s.PrevLogTerm,
		Entries:      s.Entries,
		LeaderCommit: s.LeaderCommit,
	}
	// we should copy the origin entries, and do not use the address of s.Entries
	// otherwise it may be changed during RPC!!!!
	//copyEntries := make([]Log, len(s.Entries))
	//copy(copyEntries, s.Entries)
	//args.Entries = copyEntries
}

// update follower replication state after a successful AppendEntries RPC.
func (r *Raft) updateHeartbeatInfo(serverID int,s *followerReplication){
	// index of log entry immediately preceding new ones
	// default 0 if no LogEntries
	nextIndex,_ := r.leaderState.getState(serverID)
	prevLogIndex := nextIndex - 1
	// term of prevLogIndex entry
	// default 0 if no LogEntries
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 {
		//prevLogTerm = r.getLogEntries()[prevLogIndex - 1].Term
		prevLogTerm = r.getLogTermByIndex(prevLogIndex)
	}
	r.logger.Debug("updateHeartbeatInfo check current rf state"," nextIndex",nextIndex," prevLogTerm",prevLogTerm)

	// update followerReplication
	// note that we should not update currentTerm here!!!!!!
	// since if we exit leadership and send the logs last time, the term has already changed!
	s.PrevLogIndex = prevLogIndex
	s.PrevLogTerm = prevLogTerm
	s.LeaderCommit = r.getCommitIndex()
}

// heartbeat is used to periodically invoke AppendEntries on a peer
// to ensure they don't time out. This is done async of replicate(),
// since that routine could potentially be blocked on disk IO.
func (r *Raft) heartbeat(id int, s *followerReplication, stopCh chan struct{}){
	defer func() {
		r.logger.Warn("heartbeat closed"," peer", id)
	}()
	// repeat during idle periods to prevent election timeouts
	// we should return immediately to avoid mismatch term
	for s.Term == r.getCurrentTerm(){
		r.logger.Debug("heartbeat show current followerReplication data","peer", id, " followerReplication",s)
		// Don't have these loops execute continuously without pausing
		// Wait for the next heartbeat interval or forced notify
		select {
		case <-randomTimeout(r.config().HeartbeatTimeout / 4):
		case <-stopCh:
			r.logger.Warn("replicate stopped heartbeat","leader",r.me," peer",id)
			return
		}
		// each command is sent to each peer just once.
		// heartbeat does not send entries
		if r.getState() != Leader { // has already stepped down
			r.logger.Warn("close heartbeat due to transferring to follower")
			return
		}

		// InstallSnapShot，The follower's log is smaller than leader's snapshot state,
		// send snapshot to the peer
		nextIndex,_ := r.leaderState.getState(id)
		lastIncludeIndex,_ :=r.getLastSnapshot()
		if nextIndex < lastIncludeIndex{
			r.logger.Info("InstallSnapShot to followers","leader",r.me," peer",id,
				"current term",r.getCurrentTerm()," Replication RPC term",s.Term)
			r.goFunc(func() {r.leaderSendSnapShot(id)})
			return
		}
		//r.updateHeartbeatInfo(id,s)
		args  := r.updateLastAppended(id,r.getLastIndex(),s)

		rpcState := r.processAppendEntries(id,args)
		if rpcState == Success {
			// update heartbeat timer
			s.setLastContact()
			r.logger.Debug("heartbeat success!","connect peer",id,"last contact",s.getLastContact())
		}else if rpcState == Failure{
			r.logger.Warn("heartbeat appendEntries rejected, sending older logs", "peer", id, "entries", s.Entries)
			// not here we must add this to notify main thread!!!!
			// otherwise if we only left one heartbeat goroutine, the cpu may never scheduled to main thread!!!!
			//s.stepDown <- struct{}{}
			return
		}else if rpcState == BACKTRACK{
			r.logger.Debug("heartbeat need log backtracking!","connect peer",id,"last contact",s.getLastContact())
		}
	}
}

// processAppendEntries send AppendEntriesRPC, return shouldStop(true means converted to follower, heartbeat should stop)
func (r *Raft) processAppendEntries(nodeId int, rpcArg *AppendEntriesArgs) RPCState {
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
		return OverTime
	}
	//since sendAppendEntries will block, we may no longer leader here here
	if r.getState() != Leader|| r.getCurrentTerm() != rpcArg.Term{
		r.logger.Warn("obsolete AppendEntrie request returned!!!! we should kill current goroutine")
		return Failure
	}
	//you get an AppendEntries RPC from the current leader
	//handle reply
	// note here we should not use r.currentTerm
	// since we may already changed to the follower but the heartbeat does not return !!
	currentTerm := rpcArg.Term
	currentLogs := r.getLogEntries()
	base,_ := r.getLastSnapshot()

	r.logger.Debug("receive AppendEntriesRPC, check args data and reply data","from",nodeId,", rpcArg",rpcArg,", reply",reply)
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if reply.Term > currentTerm {
		// Exit the loop, convert to follower
		r.logger.Info("turn back to follower due to existing higher term","leader ",r.me,", term: ",reply.Term, ", from server ", reply.ServerID )
		// Note here. We should update current term = reply.Term then becomes the follower!!!!
		// Assuming that a Raft cluster has three nodes, and the network of node 3 is isolated,
		// then according to the implementation of BasicRaft, the cluster will perform the following actions:
		// node 3 cannot receive Heartbeat and AppendEntries from the leader due to the network isolation,
		// so node 3 will Entering the election process, of course, the election process will not receive votes,
		// so node 3 will repeatedly time out the election; the term of node 3 will always increase.
		// When the Leader sends RPCs to Node 3, Node 3 rejects these RPCs because the sender's term is too small.
		// After the Leader receives the rejection sent by Node 3, it will increase its own Term and then become a Follower.
		// Then, the cluster starts a new election, with a high probability that the original leader will become the leader of the new round.
		r.setCurrentTerm(reply.Term)
		// notify to turn to follower
		r.setState(Follower)
		r.persist()
		return Failure
	}

	if reply.Success == true {
		if len(rpcArg.Entries) == 0{
			// if it is heartbeat AppendEntries RPC, we send empty entries,
			// so we should change nothing on leader
			return Success
		}
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		// update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally
		matchIndex := rpcArg.PrevLogIndex + uint64(len(rpcArg.Entries))
		r.logger.Info("update sever state","sever", reply.ServerID," matchIndex ",matchIndex)
		r.leaderState.updateStateSuccess(reply.ServerID,matchIndex)
		//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
		//and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
		copyMatchIndex := make(uint64Slice, len(r.peers))
		r.leaderState.indexLock.Lock()
		copy(copyMatchIndex, r.leaderState.matchIndex)
		r.leaderState.indexLock.Unlock()
		copyMatchIndex[r.me] = r.getLastIndex()
		//sort and get the middle to judge the majority
		sort.Slice(copyMatchIndex, copyMatchIndex.Less)
		N := copyMatchIndex[len(r.peers)/2]
		r.logger.Debug("check returned matchIndex","copyMatchIndex",copyMatchIndex)
		// convert to offset due to log compact
		offset := N - 1 - base
		if N > r.getCommitIndex() && currentLogs[offset].Term == currentTerm {
			r.setCommitIndex(N)
		}
		//check for commitIndex > lastLogIndex after commitIndex is updated
		r.logger.Info("check for commitIndex > lastLogIndex","commitIndex",r.getCommitIndex(),"lastLogIndex",r.getLastIndex())
		//apply to FSM
		r.applyLogCh <- struct{}{}
		return Success
	}else {
		//If AppendEntries fails because of log inconsistency:
		//decrement nextIndex and retry (§5.3)
		r.logger.Info("start log backtracking","ConflictIndex",reply.ConflictIndex," ConflictTerm",reply.ConflictTerm," current logs",r.getLogEntries())
		// The accelerated log backtracking optimization
		// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
		upperboundIndex, founded := r.lastConfictTermIndex(reply.ConflictTerm)
		if founded {
			// If it finds an entry in its log with ConflictTerm,
			// it should set nextIndex as the one beyond the index of the last entry in that term in its log.
			r.leaderState.setNextIndex(reply.ServerID, upperboundIndex)
		} else {
			r.leaderState.setNextIndex(reply.ServerID, reply.ConflictIndex)
		}
		return BACKTRACK
	}
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
			//return uint64(i + 1),founded
			return entries[i].Index,founded
		}
	}
	return 0,founded
}

//If commitIndex > lastLogIndex: increment lastLogIndex, apply log[lastLogIndex] to state machine (§5.3)
func (rf *Raft) startApplyLogs() {
	// dedicated thread calling r.app.apply from Raft
	rf.goFunc(func() {
		for {
			select {
			case <- rf.applyLogCh:
				rf.logger.Info("start applying logs,", "leader ",rf.leaderID, "term",rf.currentTerm, "at node ", rf.me,
					" lastLogIndex: ", rf.getLastIndex(), " lastApplied ", rf.getLastApplied(), " commitIndex: ", rf.getCommitIndex())

				// may only be partially submitted
				lastApplied := rf.getLastApplied()

				for lastApplied < rf.getCommitIndex(){
					newLastApplied := lastApplied + 1

					msg := ApplyMsg{}
					msg.CommandValid = true
					msg.SnapshotValid = false
					msg.CommandIndex = int(newLastApplied)

					//rf.logsLock.RLock()
					//entry := rf.logs[newLastApplied-1]
					//msg.Command = entry.Data
					//rf.logsLock.RUnlock()
					entry := rf.getEntryByOffset(newLastApplied)
					msg.Command = entry.Data

					// Update the last log since it's on disk now
					rf.logger.Debug("committed, check entry index","newLastApplied",newLastApplied,"LastApplied entry",entry)
					rf.setLastApplied(newLastApplied)

					rf.applyCh <- msg

					lastApplied = newLastApplied
				}
			case <-rf.shutdownCh:
				rf.logger.Warn("startApplyLogs goroutine shut down!!","peer",rf.me)
				return
			}
		}
	})
}

