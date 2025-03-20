package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"

	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type NodeType int

const (
	Leader NodeType = iota
	Follower
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state NodeType

	applyCh chan raftapi.ApplyMsg

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []interface{}

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	voteNum    int
	votedTimer time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      [][]byte
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.currentTerm <= args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedTimer = time.Now()
		DPrintf("[Vote] Node %d support Node %d become leader\n", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[拒绝投票] 节点 %d 任期 %d 拒绝给节点 %d 任期 %d 投票\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Entries == nil {
		DPrintf("[Handle HeartBeat] Node %d at term %d handle heartbeat from %d at term %d\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		// heartbeat
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
		} else {
			reply.Success = true
			rf.currentTerm = args.Term
			// 重置超时选举计数器
			rf.votedTimer = time.Now()
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	if rf.me != rf.votedFor {
		isLeader = false
		return index, term, isLeader
	}

	msg := raftapi.ApplyMsg{}
	msg.Command = true
	msg.Command = command
	msg.CommandIndex = len(rf.log)

	rf.applyCh <- msg

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		now := time.Now()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.state != Leader && rf.votedTimer.Before(now) {
			rf.leaderElection()
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false && rf.state == Leader {
		for i, _ := range rf.peers {
			if rf.me != i {
				//DPrintf("[HeartBeat] Leader %d send heartbeat to Node %d at term %d]\n", rf.me, i, rf.currentTerm)
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, &args, &reply)
				//if reply.Success == false {
				//	rf.state = Follower
				//	DPrintf("[Become Follower] Leader %d become Follower at term %d\n", rf.me, rf.currentTerm)
				//}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.state = Follower
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) leaderElection() {

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteNum = 0
	rf.votedTimer = time.Now()

	DPrintf("[Leader Election] Node %d start leader election at term %d\n", rf.me, rf.currentTerm)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.currentTerm,
	}
	reply := RequestVoteReply{}
	for i, _ := range rf.peers {
		if rf.me != i {
			DPrintf("[Send Vote] Node %d at term %d send vote to Node %d\n", rf.me, rf.currentTerm, i)
			rf.sendRequestVote(i, &args, &reply)
			if reply.VoteGranted {
				rf.voteNum++
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					return
				}
			}
		}
	}

	if rf.state == Candidate && rf.voteNum > len(rf.peers)/2 {
		rf.state = Leader
		DPrintf("[Become Leader]Candidate %d become leader at term %d\n", rf.me, rf.currentTerm)
		go rf.heartbeat()
	}
}
