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
	"github.com/hashicorp/go-hclog"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//candidateID that received vote in current term
	// votedFor also represent LeaderID which is the current cluster leader ID
	votedFor int32

	//each entry contains command for state machine
	// and term when entry was received by leader
	logs      []Log

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
	rpcCh <-chan RPC

	// Shutdown channel to exit, protected to prevent concurrent exits
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

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

func (r *Raft) getLeader() int32 {
	stateAddr := &r.votedFor
	return atomic.LoadInt32(stateAddr)
}

func (r *Raft) setLeader(s int32) {
	stateAddr := &r.votedFor
	atomic.StoreInt32(stateAddr, s)
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
	rf.logger.Info("the peer[", rf.me, "] state is:", rf.state)
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
	voterID ServerID
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
	rf.logger.Info("Server[", rf.me,"]: got RequestVote from candidate ",args.CandidateId,
		", args: ", args,", current currentTerm: ",rf.getCurrentTerm(),", current log: ",rf.logs,"\n")
	//reason: A network partition occurs, the candidate has OutOfDate
	if args.Term < rf.getCurrentTerm() {
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = false
		rf.logger.Debug("======= server %d got RequestVote from candidate %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.CandidateId, args, rf.logs, reply)
		return
	}

	reply.Term = args.Term
	// Increase the term if we see a newer one
	if args.Term > rf.getCurrentTerm() {
		// Ensure transition to follower
		rf.logger.Debug("lost leadership because received a requestVote with a newer term")
		rf.setState(Follower)
		rf.setCurrentTerm(args.Term)
	}
	voteFor := rf.getLeader()
	// Check if we've voted in this election before
	if args.Term == rf.getCurrentTerm() && voteFor != -1 && voteFor != args.CandidateId{
		rf.logger.Info("duplicate requestVote for same term", "term", args.Term)
		reply.VoteGranted = false
		return
	}
	// Reject if their term is older
	lastIdx, lastTerm := rf.getLastEntry()
	if lastTerm > args.LastLogTerm {
		rf.logger.Warn("rejecting vote request since our last term is greater",
			"candidate", candidate,
			"last-term", lastTerm,
			"last-candidate-term", args.Term)
		reply.VoteGranted = false
		return
	}

	if lastTerm == args.LastLogTerm && lastIdx > args.LastLogIndex {
		rf.logger.Warn("rejecting vote request since our last index is greater",
			"candidate", candidate,
			"last-index", lastIdx,
			"last-candidate-index", args.LastLogIndex)
		reply.VoteGranted = false
		return
	}
	
	//  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log,
	// grant vote (§5.2, §5.4)
	reply.VoteGranted = true
	rf.setLeader(candidate)

	// restart your election timer
	rf.setLastContact()
}


type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	// optimization: accelerated log backtracking
	ConflictTerm  int
	ConflictIndex int
}

// appendEntries is invoked when we get an append entries RPC call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){

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

	//Call() sends a request and waits for a reply.
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	//Call() is guaranteed to return (perhaps after a delay)
	rf.logger.Info("[	sendRequestVote(", rf.me,") ] : send a election to ", server)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	//Call() is guaranteed to return (perhaps after a delay)
	rf.logger.Info("[	sendAppendEntries(", rf.me,") ] : send a election to ", server)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
		atomic.StoreInt32(&rf.dead, 1)
		// Your code here, if desired.
		close(rf.shutdownCh)
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
	r.logger.Info("entering follower state", "follower", r, "leader-id", r.me)
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
				continue
			}
		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the main loop while in the candidate state.
func (r *Raft) runCandidate(){
	//Increment currentTerm
	term := r.getCurrentTerm() + 1
	r.logger.Info("entering candidate state", "node", r, "term", term)
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
				r.logger.Debug("newer term discovered, fallback to follower", "term", vote.Term)
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				r.votedFor = -1
				return
			}
			// Check if the vote is granted
			if vote.VoteGranted {
				grantedVotes++
				r.logger.Debug("vote granted", "from", vote.voterID, "term", vote.Term, "tally", grantedVotes)
			}
			// Check if we've become the leader
			if grantedVotes >= votesNeeded {
				r.logger.Info("election won", "term", vote.Term, "tally", grantedVotes)
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
	return voters/2 + 1
}



// electSelf is used to send a RequestVote RPC to all peers, and vote for
// ourself. This has the side affecting of incrementing the current term. The
// response channel returned is used to wait for all the responses (including a
// vote for ourself). This must only be called from the main thread.
func (r *Raft) electSelf() <-chan *RequestVoteReply{
	// Create a response channel
	respCh := make(chan *RequestVoteReply, len(r.peers))

	// Construct the request
	lastIdx, lastTerm := r.getLastEntry()
	req := &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  int32(r.me),
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Construct a function to ask for a vote
	askPeer := func(peerId int) {
		r.goFunc(func() {
			voteReply := &RequestVoteReply{}
			err := r.sendRequestVote(peerId, req, voteReply)
			if !err{
				r.logger.Error("failed to make requestVote RPC",
					"target", peerId,
					"error", err,
					"term", req.Term)
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
			}
		}else{
			r.logger.Debug("asking for vote", "term", req.Term, "from", serverId)
			askPeer(serverId)
		}
	}
	return respCh
}


// runLeader runs the main loop while in leader state. Do the setup here and drop into
// the leaderLoop for the hot loop.
func (r *Raft) runLeader(){

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
	rf.lastApplied = 0
	rf.state = Follower
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0

	rf.votedFor = -1
	rf.logs = make([]Log,0)

	rf.applyCh = applyCh

	rf.logger = hclog.New(&hclog.LoggerOptions{
		Name:  "my-raft",
		Level: hclog.LevelFromString(rf.config().LogLevel),
		Output: rf.config().LogOutput,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
