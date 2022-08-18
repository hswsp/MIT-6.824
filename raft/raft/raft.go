package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	minCheckInterval       = 10 * time.Millisecond
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	raftState //OOP inherit

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Shutdown channel to exit, protected to prevent concurrent exits
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	//candidateID that received vote in current term
	votedFor int32
	//the current cluster leader ID
	leaderID int32
	leaderLock sync.RWMutex

	//each entry contains command for state machine
	// and term when entry was received by leader
	//we actually use logs[index-1] to fetch log with Index = index
	logs      []Log
	logsLock sync.RWMutex

	// volatile state on leaders
	// leaderState used only while state is leader
	leaderState LeaderState

	// conf stores the current configuration to use. This is the most recent one
	// provided. All reads of config values should use the config() helper method
	// to read this safely.
	conf atomic.Value

	// lastContact is the last time we had contact from the
	// leader node. This can be used to gauge staleness.
	lastContact     time.Time
	lastContactLock sync.RWMutex

	// RPC chan comes from the transport layer
	rpcCh  chan RPC

	// applyCh is used to async send logs to the main thread to
	// be committed and applied to the FSM.
	applyCh chan ApplyMsg

	// stable is a StableStore implementation for durable state
	// It provides stable storage for many fields in raftState
	stable StableStore

	// Used for our logging
	// Logger is a user-provided logger. If nil, a logger writing to
	// LogOutput with LogLevel is used.
	logger hclog.Logger
}

func (r *Raft) config() Config {
	// Since Load() returns an interface{} type, we need to cast it first
	return r.conf.Load().(Config)
}

// LastContact returns the time of last contact by a leader.
// This only makes sense if we are currently a follower.
func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()
	return last
}

// setLastContact is used to set the last contact time to now
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

func (r *Raft) getVotedFor() int32 {
	stateAddr := &r.votedFor
	return atomic.LoadInt32(stateAddr)
}

func (r *Raft) setVotedFor(s int32) {
	stateAddr := &r.votedFor
	atomic.StoreInt32(stateAddr, s)
}

func (r *Raft) getLeader() int32 {
	stateAddr := &r.leaderID
	return atomic.LoadInt32(stateAddr)
}

func (r *Raft) setLeader(s int32) {
	stateAddr := &r.leaderID
	atomic.StoreInt32(stateAddr, s)
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
	entries :=r.logs[min(startPos,0):endIndex]
	r.logsLock.RUnlock()
	return entries
}
//cut logEntries[0:cutPos) and then append logSlice
func (r *Raft) appendLogEntries(cutPos uint64, logSlice []Log) {
	r.logsLock.Lock()
	defer r.logsLock.Unlock()
	r.logs = append(r.logs[:cutPos], logSlice...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer  rf.mu.Unlock()
	term = int(rf.currentTerm)
	rf.logger.Debug("Get state of peer","the peer[", rf.me, "] state is:", rf.state)
	if rf.state == Leader{
		isleader = true
	}else{
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
	if len(data) < 1 { // bootstrap without any state?
		return
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC handler.
// requestVote is invoked when we get a request vote RPC call.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// current node crash
	if rf.killed() {
		reply.Term = 0
		reply.VoteGranted = false
		return
	}

	candidate := args.CandidateId
	rf.logger.Info("======= got RequestVote  =======","Server[", rf.me,"]: from candidate ",candidate,
		", args: ", args,", current currentTerm: ",rf.getCurrentTerm(),", current log: ",rf.logs)
	defer rf.logger.Info("======= finished RequestVote  =======","Server[", rf.me,"]: from candidate ",candidate,
		", args: ", args,", current currentTerm: ",rf.getCurrentTerm(),", current log: ",rf.logs)

	// reply term should be currentTerm
	reply.Term = rf.getCurrentTerm()

	//reason: A network partition occurs, the candidate has OutOfDate
	if args.Term < rf.getCurrentTerm() {
		reply.VoteGranted = false
		rf.logger.Debug("======= got RequestVote =======","server",rf.me,"from candidate ",args.CandidateId,", args: ",args,", current log: ",rf.logs,", reply: ",reply)
		return
	}

	voteFor := rf.getVotedFor()
	// Check if we've voted in this election before
	if args.Term == rf.getCurrentTerm() && voteFor != -1 && voteFor != args.CandidateId{
		rf.logger.Info("duplicate requestVote for same term", "term", args.Term)
		reply.VoteGranted = false
		return
	}

	// Increase the term if we see a newer one
	if args.Term > rf.getCurrentTerm() {
		// Ensure transition to follower
		//If RPC request or response contains term T > currentTerm:
		//set currentTerm = T, convert to follower (§5.1)
		rf.logger.Debug("lost leadership because received a requestVote with a newer term")
		rf.setState(Follower)
		rf.setCurrentTerm(args.Term)
		reply.Term = args.Term
	}

	// Reject if their term is older
	lastIdx, lastTerm := rf.getLastEntry()
	if lastTerm > args.LastLogTerm {
		rf.logger.Warn("rejecting vote request since our last term is greater",
			"candidate", candidate, "last-term", lastTerm, "last-candidate-term", args.Term)
		reply.VoteGranted = false
		return
	}

	if lastTerm == args.LastLogTerm && lastIdx > args.LastLogIndex {
		rf.logger.Warn("rejecting vote request since our last index is greater",
			"candidate", candidate, "last-index", lastIdx, "last-candidate-index", args.LastLogIndex)
		reply.VoteGranted = false
		return
	}

	//  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log,
	// grant vote (§5.2, §5.4)
	reply.VoteGranted = true
	rf.setVotedFor(candidate)

	// you grant a vote to another peer. restart your election timer
	rf.setLastContact()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	rf.logger.Debug("start sending an election request","from ", rf.me,"to ", server)

	//Call() sends a request and waits for a reply.
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.logger.Debug("sending an election request returned","from ", rf.me,"to ", server)

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.logger.Info("got a new Start task","current node :",rf.me," , command: ",command )
	index := -1
	term , isLeader := rf.GetState()

	if rf.killed() {
		return index, term, false
	}

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}
	rf.logger.Debug("check original Entries info","current node :",rf.me,"Entries:",rf.getLogEntries())
	// add new entry
	rf.logsLock.Lock()
	index = len(rf.logs)
	// here our LogIndex start from 1 but we initialized from 0, so we add first.
	// not that we fetch the log entry stored in Log by Log[index - 1]
	index ++

	entry := Log{}
	entry.Index = uint64(index)
	entry.Term = rf.getCurrentTerm()
	entry.Type = LogCommand
	entry.Data = command

	rf.logs = append(rf.logs, entry)
	rf.logsLock.Unlock()

	rf.logger.Debug("check current Entries info","current node :",rf.me,"Entries:",rf.getLogEntries())

	rf.persist()
	rf.leaderState.commitCh <- struct{}{}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	rf.shutdownLock.Lock()
	defer rf.shutdownLock.Unlock()
	if !rf.killed() {
		// Your code here, if desired.
		close(rf.shutdownCh)
		rf.setState(Shutdown)
		rf.waitShutdown()
		atomic.StoreInt32(&rf.dead, 1)
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//main server loop.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// Check if we are doing a shutdown
		select {
		case <-rf.shutdownCh:
			// Clear the leader to prevent forwarding
			rf.setLeader(-1)
			return
		default:
		}

		switch rf.getState() {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}
// runFollower runs the main loop while in the follower state.
func (r *Raft) runFollower(){
	//init
	r.setVotedFor(-1)
	r.logger.Info("entering follower state", "follower", r.me, "leader-id", r.getLeader())
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout)

	for r.getState() == Follower {
		select {
		case <-heartbeatTimer:
			// Restart the heartbeat timer
			hbTimeout := r.config().HeartbeatTimeout
			heartbeatTimer = randomTimeout(hbTimeout)

			// Check if we have had a successful contact
			lastContact := r.LastContact()
			if time.Now().Sub(lastContact) < hbTimeout {
				r.logger.Info("normal heartbeat, check current state","currentTerm",r.getCurrentTerm(),
					"votedFor",r.getVotedFor(),"leaderID",r.getLeader(),"logs",r.getLogEntries())
				continue
			}
			// Heartbeat failed! Transition to the candidate state
			//If election timeout elapses without receiving AppendEntries RPC from current leader
			//or granting vote to candidate: convert to candidate
			r.setLeader(-1)
			r.setState(Candidate)
		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the main loop while in the candidate state.
func (r *Raft) runCandidate(){
	//Increment currentTerm
	term := r.getCurrentTerm() + 1
	r.logger.Info("entering candidate state", "node", r.me, "term", term)
	// Start vote for us, and set a timeout
	voteCh := r.electSelf()
	// At the beginning of each election round, reset the election timeout
	electionTimeout := r.config().ElectionTimeout
	electionTimer := randomTimeout(electionTimeout)
	// Tally the votes, need a simple majority
	grantedVotes := 0
	votesNeeded := r.quorumSize()
	for r.getState() == Candidate {
		select {
		case vote := <-voteCh:
			// If RPC request or response contains term T > currentTerm:
			//set currentTerm = T, convert to follower (§5.1)
			if vote.Term > r.getCurrentTerm() {
				r.logger.Warn("newer term discovered, fallback to follower", "term", vote.Term)
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				r.setLeader(-1)
				return
			}
			// Check if the vote is granted
			if vote.VoteGranted {
				grantedVotes++
				r.logger.Debug("vote granted", "from", vote.VoterID,
					"term", vote.Term, "tally", grantedVotes)
			}
			// Check if we've become the leader
			if grantedVotes >= votesNeeded {
				r.logger.Info("election won","server [",r.me,"], term", vote.Term, "tally", grantedVotes)
				r.setState(Leader)
				r.setLeader(int32(r.me))
				return
			}
		case <-electionTimer:
			//If election timeout elapses: start new election
			r.logger.Warn("Election timeout reached, restarting election")
			return
		case <-r.shutdownCh:
			return
		}
	}
}

// quorumSize is used to return the quorum size. This must only be called on
// the main thread.
func (r *Raft) quorumSize() int {
	voters := len(r.peers)/2
	r.logger.Info("majority vote","quorum size is ",voters + 1)
	return voters + 1
}

// electSelf is used to send a RequestVote RPC to all peers, and vote for
// ourself. This has the side affecting of incrementing the current term. The
// response channel returned is used to wait for all the responses (including a
// vote for ourself). This must only be called from the main thread.
func (r *Raft) electSelf() <-chan *RequestVoteReply{
	// Increment the term
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	// Create a response channel
	respCh := make(chan *RequestVoteReply, len(r.peers))

	// Construct the request
	lastIdx, lastTerm := r.getLastEntry()
	req := &RequestVoteArgs{
		Term:         r.getCurrentTerm(),
		CandidateId:  int32(r.me),
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Construct a function to ask for a vote
	askPeer := func(peerId int) {
		r.goFunc(func() {
			voteReply := &RequestVoteReply{}
			voteReply.VoterID = fmt.Sprintf("%d", peerId)
			err := r.sendRequestVote(peerId, req, voteReply)
			if !err{
				r.logger.Error("failed to make requestVote RPC", "target", peerId,
					"error", err, "term", req.Term)
				voteReply.Term = req.Term
				voteReply.VoteGranted = false
			}
			respCh <- voteReply
		})
	}

	// For each peer, request a vote
	for serverId,_:=range r.peers{
		// vote for myself
		if serverId==r.me {
			r.logger.Debug("voting for self", "term", req.Term, "id", r.me)
			// Include our own vote
			respCh <-&RequestVoteReply{
				Term:        req.Term,
				VoteGranted: true,
				VoterID:  fmt.Sprintf("%d", serverId),
			}
			r.setVotedFor(int32(r.me))
		}else{
			r.logger.Debug(" asking for vote","node ",r.me,  "term", req.Term, "from", serverId)
			askPeer(serverId)
		}
	}
	return respCh
}


// runLeader runs the main loop while in leader state. Do the setup here and drop into
// the leaderLoop for the hot loop.
func (r *Raft) runLeader(){
	// setup leader state. This is only supposed to be accessed within the leaderloop.
	r.logger.Info("entering leader state ", "leader", r.me,", current term ",r.getCurrentTerm())
	r.setupLeaderState()
	// Cleanup state on step down
	defer func() {
		r.logger.Info("start exiting leader state ", "leader", r.me,", current term ",r.getCurrentTerm())
		// Since we were the leader previously, we update our
		// last contact time when we step down, so that we are not
		// reporting a last contact time from before we were the
		// leader. Otherwise, to a client it would seem our data
		// is extremely stale.
		r.setLastContact()
		// Stop replication
		for _, p := range r.leaderState.replState {
			close(p.stopCh)
		}
		//all of the stepDown in followerReplication are controlled by leaderState
		close(r.leaderState.stepDown)

		r.leaderState.waitStepDown()
		// Clear all the state
		r.leaderState.nextIndex = nil
		r.leaderState.matchIndex = nil
		r.leaderState.replState = nil
		r.leaderState.stepDown = nil
		r.leaderState.commitCh = nil
		// If we are stepping down for some reason, no known leader.
		// We may have stepped down due to an RPC call, which would
		// provide the leader, so we cannot always blank this out.
		r.leaderLock.Lock()
		if r.getLeader() == int32(r.me){
			r.setLeader(-1)
		}
		r.leaderLock.Unlock()

		r.logger.Info("lost leader state","leader", r.me)
	}()

	// Start a replication routine for each peer
	r.startStopReplication()

	// Sit in the leader loop until we step down
	r.leaderLoop()
}

func (r *Raft) setupLeaderState() {
	r.leaderState.nextIndex = make([]uint64, len(r.peers))
	r.leaderState.matchIndex = make([]uint64, len(r.peers))
	r.leaderState.replState = make(map[int] *followerReplication)
	r.leaderState.stepDown = make(chan struct{})
	r.leaderState.commitCh = make(chan struct{})
	lastIdx := r.getLastIndex()
	for i:=0; i<len(r.peers); i++ {
		//initialized to leader last log index + 1
		r.leaderState.nextIndex[i] = lastIdx + 1
		//matchIndex is initialized to 0
		r.leaderState.matchIndex[i] = 0
	}
}

// startStopReplication will set up state and start asynchronous replication to
// new peers, and stop replication to removed peers. Before removing a peer,
// it'll instruct the replication routines to try to replicate to the current
// index. This must only be called from the main thread.
func (r *Raft) startStopReplication(){
	inConfig := make(map[int]bool, len(r.peers))
	lastIdx := r.getLastIndex()
	//Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
	// Start replication goroutines that need starting
	for serverID, _ := range r.peers {
		if serverID == r.me {
			continue
		}
		inConfig[serverID] = true

		r.mu.Lock()
		//index of log entry immediately preceding new ones
		prevLogIndex := r.leaderState.nextIndex[serverID] - 1
		//term of prevLogIndex entry
		prevLogTerm := uint64(0)
		if prevLogIndex > 0 {
			prevLogTerm = r.logs[prevLogIndex - 1].Term
		}
		r.mu.Unlock()

		r.logger.Debug("initial AppendEntries RPCs (heartbeat)","peer", serverID, " prevLogIndex", prevLogIndex,"prevLogTerm", prevLogTerm)
		s, ok := r.leaderState.replState[serverID]
		if !ok{
			s = &followerReplication{
				Term:         r.getCurrentTerm(),
				LeaderId:     int32(r.me),
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				triggerCh:    make(chan struct{}, 1),
				LeaderCommit: r.getCommitIndex(),
				LastContact:  time.Now(),
				StepDown:     r.leaderState.stepDown,
				stopCh: 	  make(chan uint64),
			}
			r.logger.Info("added peer, starting replication", "peer", serverID)
			r.leaderState.replState[serverID] = s
			// Note: here you should copy current serverID to local variable in case of concurrency!!!
			peer := serverID
			r.leaderState.goFunc(func() {
				r.logger.Debug("added peer ","peerId = ", peer, "args is ",s)
				r.replicate(peer, s)
			})
		}else{
			r.logger.Info("already replicate to the peer, check leader", "peer", serverID)
			if s.getLeaderId()!= int32(serverID){
				s.setLeaderId(int32(serverID))
			}
		}
	}
	// Stop replication goroutines that need stopping
	for serverID, repl := range r.leaderState.replState {
		if inConfig[serverID] {
			continue
		}
		// Replicate up to lastIdx and stop
		r.logger.Info("removed peer, stopping replication", "peer", serverID, "last-index", lastIdx)
		repl.stopCh <- lastIdx
		close(repl.stopCh)
		delete(r.leaderState.replState, serverID)
	}
}

// leaderLoop is the hot loop for a leader. It is invoked
// after all the various leader setup is done.
func (r *Raft) leaderLoop() {

	// This is only used for the first lease check, we reload lease below
	// based on the current config value.
	lease := time.After(r.config().LeaderLeaseTimeout)
	for r.getState() == Leader {
		select {
		case <-r.leaderState.stepDown:
			r.setState(Follower)
		case <-lease:
			r.logger.Debug("Check if we've exceeded the lease, potentially stepping down","leader = ",r.me)
			// Check if we've exceeded the lease, potentially stepping down
			maxDiff := r.checkLeaderLease()

			// Next check interval should adjust for the last node we've
			// contacted, without going negative
			checkInterval := r.config().LeaderLeaseTimeout - maxDiff
			if checkInterval < minCheckInterval {
				checkInterval = minCheckInterval
			}

			// Renew the lease timer
			lease = time.After(checkInterval)
		case <-r.leaderState.commitCh:
			r.logger.Debug("new logs added, should propagate to followers","leader ",r.me)
			for peerID,_ :=range r.peers {
				if peerID == r.me {
					continue
				}
				f := r.leaderState.replState[peerID]
				f.triggerCh <- struct{}{}
			}
		case <-r.shutdownCh:
			return
		}
	}
}

// checkLeaderLease is used to check if we can contact a quorum of nodes
// within the last leader lease interval. If not, we need to step down,
// as we may have lost connectivity. Returns the maximum duration without
// contact. This must only be called from the main thread.
func (r *Raft) checkLeaderLease() time.Duration {
	// Track contacted nodes, we can always contact ourself
	contacted := 0
	// Store lease timeout for this one check invocation as we need to refer to it
	// in the loop and would be confusing if it ever becomes reloadable and
	// changes between iterations below.
	leaseTimeout := r.config().LeaderLeaseTimeout

	// Check each follower
	var maxDiff time.Duration
	now := time.Now()
	for id,_ :=range r.peers{
		if id==r.me {
			contacted++
			continue
		}
		f := r.leaderState.replState[id]
		diff := now.Sub(f.getLastContact())
		if diff <= leaseTimeout {
			contacted++
			if diff > maxDiff {
				maxDiff = diff
			}
		} else {
			// Log at least once at high value, then debug. Otherwise it gets very verbose.
			if diff <= 3*leaseTimeout {
				r.logger.Warn("failed to contact", "server-id", id, "time", diff)
			} else {
				r.logger.Debug("failed to contact", "server-id", id, "time", diff)
			}
		}
	}
	// Verify we can contact a quorum
	quorum := r.quorumSize()
	if contacted < quorum {
		r.logger.Warn("failed to contact quorum of nodes, stepping down")
		r.setLeader(-1)
		r.setState(Follower)
	}
	return maxDiff
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	atomic.StoreInt32(&rf.dead, 0)
	rf.shutdownCh = make(chan struct{})

	rf.conf.Store(DefaultConfig())

	rf.currentTerm = 0
	rf.commitIndex = 0

	rf.state = Follower

	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	rf.lastApplied = 0
	rf.lastLogTerm = 0

	rf.votedFor = -1
	rf.logs = make([]Log,0)

	rf.applyCh = applyCh

	rf.logger = hclog.New(&hclog.LoggerOptions{
		Name:  fmt.Sprintf("my-raft-%d", me),
		Level: hclog.LevelFromString(rf.config().LogLevel),
		Output: rf.config().LogOutput,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
