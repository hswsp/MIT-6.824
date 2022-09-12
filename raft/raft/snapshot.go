package raft

import "time"

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}
	rf.logger.Debug("starting sending an SnapShot","from ", rf.me," to ", server)
	rstChan := make(chan (bool))
	ok := false
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
		rstChan <- ok
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(rf.config().HeartbeatTimeout):
		//call rpc timeout!!!
		rf.logger.Debug("sent an SnapShot timeout!!","from ", rf.me," to ", server)
	}
	return ok
}

// InstallSnapShot is invoked when we get a InstallSnapshot RPC call.
// We must be in the follower state for this, since it means we are
// too far behind a leader for log replay. This must only be called
// from the main thread.
func (r *Raft) InstallSnapShot(req *InstallSnapshotRequest,reply *InstallSnapshotReply) {
	// Setup a response
	reply.Term = r.getCurrentTerm()
	reply.Success = false

	// Reply immediately if term < currentTerm
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("ignoring InstallSnapShot request with older term than current term",
			"request-term", req.Term, "current-term", r.getCurrentTerm())
		return
	}
	lastIncludeIndex,_ :=r.getLastSnapshot()
	if req.LastLogIndex <= lastIncludeIndex{
		r.logger.Info("ignoring InstallSnapShot request with older snapshot index than current index",
			"request-index", req.LastLogIndex, "current-index", lastIncludeIndex)
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		// Ensure transition to follower
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		reply.Term = req.Term
	}
	// Save the current leader
	r.setLeader(req.LeaderId)

	// Save snapshot file, discard any existing or partial snapshot with a smaller index
	r.installLastSnapshot(req.LastLogIndex,req.LastLogTerm)

	//reset commitIndex、lastApplied
	if req.LastLogIndex > r.getCommitIndex() {
		r.setCommitIndex(req.LastLogIndex)
	}
	// Update the lastApplied so we don't replay old logs
	if req.LastLogIndex > r.getLastApplied() {
		r.setLastApplied(req.LastLogIndex)
	}
	// Persistent snapshot information
	r.persister.SaveStateAndSnapshot(r.persistData(), req.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotTerm:  int(req.LastLogTerm),
		SnapshotIndex: int(req.LastLogIndex),
	}
	r.applyCh <- msg

	r.logger.Info("Installed remote snapshot")
	reply.Success = true
	r.setLastContact()
}

func (rf *Raft) leaderSendSnapShot(server int){
	lastLogIndex,lastLogTerm := rf.getLastSnapshot()
	args := &InstallSnapshotRequest{
		Term:     rf.getCurrentTerm(),
		LeaderId: int32(rf.me),
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
		Data: rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	res := rf.sendSnapShot(server, args, reply)
	if !res {
		rf.logger.Warn("sendSnapShot error!")
		return
	}

	if rf.getState() != Leader || rf.getCurrentTerm()!= args.Term{
		rf.logger.Warn("we have already not been the leader! SendSnapShot return!", "peer",rf.me)
		return
	}

	// If the returned term is larger than ourself, it means that our own data is not suitable.
	if reply.Term > rf.getCurrentTerm(){
		rf.setState(Follower)
		rf.setVotedFor(-1)
		rf.persist()
		return
	}

	rf.leaderState.updateStateSuccess(server, args.LastLogIndex)

}


func (r *Raft) installLastSnapshot(lastLogIndex uint64,lastLogTerm uint64)  {

	r.lastLock.Lock()
	lastIncludeIndex := r.lastSnapshotIndex
	r.lastLock.Unlock()

	offsetIndex := int64(lastLogIndex) - int64(1 + lastIncludeIndex)
	// last snapshot including index, so + 1.
	cutPos := uint64(offsetIndex + 1)

	r.logsLock.Lock()
	// copy on write
	entries :=r.logs
	r.logger.Warn("check log array before cut","current logs",entries,"cutPos",cutPos)

	//typically cutPos > originLen due to too old logs in follower
	sLogs := make([]Log, 0)
	originLen := uint64(len(entries))
	if cutPos < originLen {
		sLogs = append(sLogs,r.logs[cutPos:]...)
	}
	r.logs = sLogs
	r.logger.Warn("check log array after cut","current logs",r.logs)
	r.logsLock.Unlock()

	// update lastSnapshotIndex/term
	r.lastLock.Lock()
	r.lastSnapshotIndex = lastLogIndex
	r.lastSnapshotTerm = lastLogTerm
	r.lastLock.Unlock()

}