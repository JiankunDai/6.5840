package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sort"

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

const (
	ElectionTimeout      = 100 * time.Millisecond
	AppendEntriesTimeout = 80 * time.Millisecond
	CommitTimeout        = 100 * time.Millisecond
	SnapshotTimeout      = 100 * time.Millisecond
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
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	voteNum       int
	electionTimer time.Time

	appendMutex sync.Mutex
	commitMutex sync.Mutex
}

type LogEntry struct {
	Term    int
	Command interface{}
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
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[RejectVoteTerm] 节点 %d 任期 %d 拒绝给节点 %d 任期 %d 投票\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// 自己想象的代码，错误
	//被分区的节点会重复多次发起选举，导致term增大，但其日志并没有更新，故不能为其直接投票
	//此时返回currentTerm会导致当前的leader退位，集群开始重新选举
	//但是缺失部分日志的节点后续也不会被选举为leader
	//lastLogIndex, lastTerm := rf.getLastLogEntry()
	//if args.LastLogTerm < lastTerm || args.LastLogIndex < lastLogIndex {
	//	reply.VoteGranted = false
	//	reply.Term = rf.currentTerm
	//	DPrintf("[RejectVoteLog] 节点 %d 任期 %d lastLogIndex %d 拒绝给节点 %d 任期 %d lastLogIndex %d 投票\n", rf.me, rf.currentTerm, lastLogIndex, args.CandidateId, args.Term, args.LastLogIndex)
	//	return
	//}

	// 自己想象的代码，错误
	//if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
	//	// 当前任期已经投票给其他节点，拒绝投票
	//	reply.VoteGranted = false
	//	reply.Term = rf.currentTerm
	//	DPrintf("[RejectVoteAnother] 节点 %d 任期 %d 拒绝给节点 %d 任期 %d 投票\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	//	return
	//}
	if (rf.votedFor != -1 && rf.votedFor != args.CandidateId) &&
		!rf.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[RejectVoteAnother] 节点 %d 任期 %d 拒绝给节点 %d 任期 %d 投票\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	reply.VoteGranted = true
	rf.becomeFollower(args.Term)
	rf.votedFor = args.CandidateId
	rf.electionTimer = time.Now()
	DPrintf("[Vote] Node %d Term %d support Node %d Term %d become leader\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
}

func (rf *Raft) isCandidateLogUpToDate(candidateLastLogTerm int, candidateLastLogIndex int) bool {
	lastLogIndex, lastLogTerm := rf.getLastLogEntry()

	// 比较最后一个条目的任期
	if candidateLastLogTerm > lastLogTerm {
		return true
	}
	if candidateLastLogTerm < lastLogTerm {
		return false
	}

	// 如果任期相同，比较日志长度
	return candidateLastLogIndex >= lastLogIndex
}

// AppendEntries Append Entries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	DPrintf("[HandleAppendEntries] Node %d at term %d, log = %v, commitIndex %d handle appendEntries %v, PrevLogIndex = %d, PrevLogTerm = %d, from %d at term %d, leaderCommit %d\n",
		rf.me, rf.currentTerm, rf.log, rf.commitIndex, args.Entries, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId, args.Term, args.LeaderCommit)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[NewTerm1] Node %d Term %d big than leader %d Term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	//DPrintf("[Handle HeartBeat] Node %d at term %d handle heartbeat from %d at term %d\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	rf.currentTerm = args.Term
	// 重置超时选举计数器
	rf.electionTimer = time.Now()
	rf.state = Follower
	//DPrintf("[StateChange] Node %d state change to %v", rf.me, rf.state)
	rf.votedFor = args.LeaderId
	rf.voteNum = 0

	//lastLogIndex, lastTerm := rf.getLastLogEntry()
	//if lastLogIndex < args.PrevLogIndex || lastTerm != args.PrevLogTerm {
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 当前日志中不存在prevLogIndex
		DPrintf("[当前日志中不存在prevLogIndex] Node %d log %v, args.PrevLogIndex = %d, args.PrevLogTerm = %d\n", rf.me, rf.log, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		return
	}

	reply.Success = true
	if len(rf.log) > args.PrevLogIndex+1 && args.Entries != nil && args.Entries[0].Term != rf.log[args.PrevLogIndex+1].Term {
		// existing entry conflicts with new one(same index but different term)
		// delete the existing entry and all that follow it
		DPrintf("[LogConflict] Node %d log is %v", rf.me, rf.log)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		DPrintf("[Update CommitIndex]Node %d commitIndex update, now is %d\n", rf.me, rf.commitIndex)
	}
	//DPrintf("[Handle AppendEntries Success] Node %d handle appendEntries success, now log is %v", rf.me, rf.log)
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
	// Your code here (3B).
	if rf.state != Leader {
		return -1, -1, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	index := len(rf.log)
	rf.log = append(rf.log, entry)
	term := rf.currentTerm

	DPrintf("[Start] %v on Node %d, Term %d, Log = %v", command, rf.me, rf.currentTerm, rf.log)
	return index, term, true
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	if index < 0 || index >= len(rf.log) {
		return LogEntry{}
	}
	return rf.log[index]
}

// getLastLogEntry
func (rf *Raft) getLastLogEntry() (int, int) {
	lastLogIndex := len(rf.log) - 1
	lastTerm := rf.log[lastLogIndex].Term
	return lastLogIndex, lastTerm
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
		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.state != Leader && rf.electionTimer.Before(now) {
			//DPrintf("Node %d now = %v, votedTimer = %v", rf.me, now.Format(time.StampMilli), rf.votedTimer.Format(time.StampMilli))
			rf.leaderElection()
		}
	}
}

func (rf *Raft) appendEntriesTicker() {
	for rf.killed() == false {
		if rf.state == Leader {
			DPrintf("[appendEntriesTicker] Node %d log is %v", rf.me, rf.log)
			// 先更新自己的nextIndex和matchIndex
			// nextIndex关系到leader会从日志哪里开始发送，对于leader本身来说应该无关紧要
			rf.matchIndex[rf.me] = len(rf.log) - 1
			rf.nextIndex[rf.me] = len(rf.log)

			for i, _ := range rf.peers {
				if rf.me != i && rf.state == Leader {
					go func(server int) {
						rf.mu.Lock()
						prevLogIndex := rf.nextIndex[server] - 1
						prevLogTerm := rf.log[prevLogIndex].Term
						entries := rf.log[rf.nextIndex[server]:]
						if len(entries) != 0 {
							DPrintf("[AppendEntries] Leader %d want append entries to Node %d at term %d, nextIndex is %v, matchIndex is %v, entries = %v \n", rf.me, server, rf.currentTerm, rf.nextIndex, rf.matchIndex, entries)
						}
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,
							Entries:      entries,
							LeaderCommit: rf.commitIndex,
						}
						rf.mu.Unlock()
						reply := AppendEntriesReply{}
						ok := rf.sendAppendEntries(server, &args, &reply)
						if ok {
							rf.mu.Lock()
							if rf.state != Leader || rf.currentTerm != args.Term {
								// 服务器被来自旧任期的 RPC搞混
								// 比较当前任期与你发送原始 RPC 时的任期。如果两者不同，则丢弃回复并返回。
								return
							}
							if reply.Success {
								DPrintf("[AppendSuccess] Node %d append entries to Node %d success, entries = %v", rf.me, server, entries)
								// 正确的事情是将 matchIndex 更新为你最初在 RPC 参数中发送的 prevLogIndex + len(entries[])
								rf.nextIndex[server] = prevLogIndex + len(entries) + 1
								rf.matchIndex[server] = prevLogIndex + len(entries)
							} else {
								DPrintf("[AppendFailed] Node %d append entries to Node %d failed, entries = %v, reply = %v", rf.me, server, entries, reply)
								if reply.Term > rf.currentTerm {
									// 有任期更新的节点出现，退位，集群开始重新选举
									DPrintf("[BecomeFollower] Node(%d) at Term(%d) become follower because big term(%d) from Node(%d)", rf.me, rf.currentTerm, reply.Term, server)
									rf.becomeFollower(reply.Term)
									return
								} else {
									// 日志不一致，需要追溯历史日志
									DPrintf("[NeedAppendHistory] Node %d append entries to Node %d failed, nextIndex is %v\n", rf.me, server, rf.nextIndex)
									if rf.nextIndex[server] > 2 {
										rf.nextIndex[server]--
									}
								}
							}
							rf.mu.Unlock()
						} else {
							//DPrintf("[NetworkError] Node %d append entries to %d occured network error", rf.me, server)
						}
					}(i)

				}
			}
		}
		time.Sleep(AppendEntriesTimeout)
	}
}

func (rf *Raft) commitTicker() {
	for rf.killed() == false {
		rf.commitMutex.Lock()
		if rf.state == Leader {
			DPrintf("[WantApplyMsg] Node %d apply msg, lastApplied = %d, commitIndex = %d, matchIndex = %v", rf.me, rf.lastApplied, rf.commitIndex, rf.matchIndex)

			rf.mu.Lock()
			// 根据follower复制情况更新commitIndex
			matchIndex := make([]int, len(rf.matchIndex))
			copy(matchIndex, rf.matchIndex)
			sort.Ints(matchIndex)
			//DPrintf("[CommitTicker] Match index = %v\n", matchIndex)
			rf.commitIndex = max(matchIndex[len(rf.peers)/2], rf.commitIndex)
			rf.mu.Unlock()
		}
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := raftapi.ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[i].Command,
					CommandIndex:  i,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				DPrintf("[Apply Msg] Node %d apply msg is %v", rf.me, msg)
				rf.applyCh <- msg
			}
			rf.lastApplied = rf.commitIndex
		}

		rf.commitMutex.Unlock()
		time.Sleep(CommitTimeout)
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
	rf.state = Follower
	rf.votedFor = -1
	rf.electionTimer = time.Now()

	// (3B)
	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	rf.commitIndex = 0
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	rf.lastApplied = 0

	rf.log = []LogEntry{{Term: 0}}

	rf.init2Index()

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start ticker goroutine to start append entries
	go rf.appendEntriesTicker()

	go rf.commitTicker()
	return rf
}

func (rf *Raft) init2Index() {
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) leaderElection() {

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteNum++
	rf.electionTimer = time.Now()

	DPrintf("[LeaderElection] Node %d start leader election at term %d\n", rf.me, rf.currentTerm)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.currentTerm,
	}
	reply := RequestVoteReply{}
	for i, _ := range rf.peers {
		if rf.me != i {
			go func(server int) {
				DPrintf("[Send Vote] Node %d state %v at term %d send vote to Node %d\n", rf.me, rf.state, rf.currentTerm, server)
				ok := rf.sendRequestVote(server, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok {
					if rf.state != Candidate {
						DPrintf("[StrangeChangeState], %v", rf.state)
						return
					}
					if reply.VoteGranted {
						rf.voteNum++
						DPrintf("[VoteStatistic] Node %d, %d %d", rf.me, rf.voteNum, len(rf.peers)/2)
						if rf.voteNum > len(rf.peers)/2 {
							rf.becomeLeader()
							rf.init2Index()
							DPrintf("[BecomeLeader]Candidate %d become leader at term %d\n", rf.me, rf.currentTerm)
							//go rf.heartbeat()
						}
					} else {
						if reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)
							return
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) becomeLeader() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.state = Leader
	rf.votedFor = rf.me
	rf.voteNum = 0
}

func (rf *Raft) becomeFollower(term int) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	rf.voteNum = 0
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = rf.me
	rf.state = Candidate
}
