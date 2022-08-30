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


// installSnapshot is invoked when we get a InstallSnapshot RPC call.
// We must be in the follower state for this, since it means we are
// too far behind a leader for log replay. This must only be called
// from the main thread.
func (r *Raft) installSnapshot(req *InstallSnapshotRequest,reply *InstallSnapshotReply) {
	// Setup a response
	reply.Term = r.getCurrentTerm()
	reply.Success = false

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("ignoring installSnapshot request with older term than current term",
			"request-term", req.Term, "current-term", r.getCurrentTerm())
		return
	}
	lastIncludeIndex,_ :=r.getLastSnapshot()
	if req.LastLogIndex <= lastIncludeIndex{
		r.logger.Info("ignoring installSnapshot request with older snapshot index than current index",
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

	// Cut the logs after the snapshot and apply logs directly before the snapshot
	offsetIndex := req.LastLogIndex - lastIncludeIndex
	r.cutLogEntries(offsetIndex)
	r.setLastSnapshot(req.LastLogIndex,req.LastLogTerm)

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