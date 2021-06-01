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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	Initial State = iota
	Follower
	Candidate
	Leader
)

const (
	HeartBeatTimer     = 150
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 600
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistant state on servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// volatile state on servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	netIndex   []int
	matchIndex []int

	// other required variables
	state                State
	lastHeardTime        time.Time
	electionTimeOutForMe time.Duration
}

type LogEntry struct {
	term    int
	command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// // Your code here (2A).
	// return term, isleader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArg struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// func (rf *Raft) tick(last time.Time, sle time.Duration) {
// 	if last.Add(sle).Before(time.Now()) {
// 		DPrintf("election timeout, starting new election")
// 		rf.mu.Lock()
// 		rf.stateChange(Candidate)
// 		rf.mu.Unlock()
// 	}
// }

func (rf *Raft) electionStarts() {

	timeToSleepInMs := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
	timeToSleep := time.Duration(timeToSleepInMs) * time.Millisecond

	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeardTime = time.Now()
	DPrintf("%d Attempting election at term %d, ", rf.me, rf.currentTerm)
	totalVotes := 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	term := rf.currentTerm
	tc := 0

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, term int) {
				// go rf.tick(rf.lastHeardTime, timeToSleep)
				if rf.lastHeardTime.Add(timeToSleep).Before(time.Now()) {
					DPrintf("election timeout, starting new election")
					rf.stateChange(Candidate)
				}
				reply := RequestVoteReply{}
				DPrintf("%d sending request vote to server %d", rf.me, server)
				// rf.mu.Lock()
				result := rf.sendRequestVote(server, &args, &reply)
				DPrintf("%d received request vote from server %d", rf.me, server)
				// rf.mu.Unlock()

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !result {
					tc++
					if 2*tc > len(rf.peers) {
						rf.stateChange(Candidate)
					}
					DPrintf("Result is false for server %d for term %d", rf.me, rf.currentTerm)
					return
				}

				if reply.Term > rf.currentTerm {
					DPrintf("%d server's reply term is %d higher than current term %d", rf.me, reply.Term, rf.currentTerm)
					rf.currentTerm = term
					rf.votedFor = -1
					rf.stateChange(Follower)
					return
				}

				if term < reply.Term {
					DPrintf("Term is less than reply.term for server %d", rf.me)
					return
				}

				if reply.VoteGranted {
					DPrintf("Inside reply.VoteGranted for server %d", rf.me)
					totalVotes++
					if rf.state != Candidate || rf.currentTerm != term {
						return
					}
					if 2*totalVotes > len(rf.peers) {
						DPrintf("%d server got %d votes.. and now being elected as leader (current Term %d)", rf.me, totalVotes, rf.currentTerm)
						// DPrintf("%d server elected as a Leader..", rf.me)
						rf.stateChange(Leader)
					}
				}

			}(i, term)
		}
	}

}

func (rf *Raft) stateChange(state State) {

	if state == Follower {
		if rf.state != state {
			// DPrintf("Follower State not equal... Current state : %v ", rf.state)
			go rf.checkTimer()
		}
		DPrintf("%d server is getting converted to Follower state", rf.me)
		rf.state = state
	} else if state == Candidate {
		go rf.electionStarts()
	} else if state == Leader {
		if rf.state != state {
			DPrintf("Leader state not equal... Current state : %v and passed state : %v ", rf.state, state)
			go rf.startHeartBeat()
		}
		rf.state = state
	}
}

func (rf *Raft) LeaderSendAppend(server int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go rf.SendAppendEntries(server)
		time.Sleep(HeartBeatTimer * time.Millisecond)
	}
}

func (rf *Raft) SendAppendEntries(server int) {
	rf.mu.Lock()
	appendArgs := AppendEntriesArg{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	appendReply := AppendEntriesReply{}
	rf.mu.Unlock()
	DPrintf("%d sending append entry to server %d", rf.me, server)
	result := rf.sendAppendEntries(server, &appendArgs, &appendReply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !result {
		return
	}

	if appendReply.Term > rf.currentTerm {
		rf.stateChange(Follower)
		return
	}
}

func (rf *Raft) startHeartBeat() {
	for x := 0; x < len(rf.peers); x++ {
		if x != rf.me {
			go rf.LeaderSendAppend(x)
		}
	}
}

func (rf *Raft) checkTimer() {
	for {
		timeToSleepInMs := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
		timeToSleep := time.Duration(timeToSleepInMs) * time.Millisecond
		time.Sleep(timeToSleep)

		rf.mu.Lock()

		if rf.state != Follower || rf.killed() {
			DPrintf("%d no longer follower, canceling election timer...", rf.me)
			rf.mu.Unlock()
			return
		}

		if time.Now().Sub(rf.lastHeardTime) > timeToSleep {
			DPrintf("Not heard the heartbeat.. time for election by server %v... Term is %d", rf.me, rf.currentTerm)
			// rf.stateChange(Candidate)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.stateChange(Candidate)
	rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d receives request vote from %d for term %d", rf.me, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		DPrintf("%d server Into vote granted false ", rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("updates term to %v, converting to follower", args.Term)
		rf.lastHeardTime = time.Now()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.stateChange(Follower)
	}

	reply.Term = rf.currentTerm
	// also add a check whether candidate's log is atleast p-to-date as reciever's log
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("Yes vote to %d", args.CandidateId)
	} else {
		reply.VoteGranted = false
		DPrintf("No vote to %d", args.CandidateId)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d sent Append entries to server %d for term %d", args.LeaderId, rf.me, args.Term)
	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.lastHeardTime = time.Now()
		rf.votedFor = -1
		if rf.state != Follower {
			DPrintf("Changing %d server into follower state...", rf.me)
			rf.stateChange(Follower)
		}
	} else {
		reply.Success = false
		DPrintf("The append entry is not valid")
	}
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
	DPrintf("Anser for server %d", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	// DPrintf("Inside the sendAppendEntries for server %d", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Initial
	rf.lastHeardTime = time.Now()
	// temp := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
	// fmt.Printf("var1 = %T\n", temp)
	// rf.electionTimeOutForMe = time.Duration(temp) * time.Millisecond
	// go rf.startLeaderElection()
	DPrintf("Initialization done of server : %v", rf.me)
	rf.mu.Lock()
	rf.stateChange(Follower)
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
