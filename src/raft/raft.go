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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

const (	
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

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

type LogEntry struct {
	Command interface{}
	Term int
	Index int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers:
	currentTerm int
	votedFor int
	log []LogEntry
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	currentState atomic.Int32
	// Volatile state on leaders:
	nextIndex []int
	matchIndex []int
	// Channels
	resetCh chan bool // channel to reset the election timer  only used by followers
	voteCh chan RequestVoteReply // channel to count the votes only used by candidates
	isLeaderCh chan int // channel to notify the ticker that the server becomes leader
	notLeaderCh chan bool // channel to notify the ticker that the server is not leader
	// condition variable to detect timeout
	timeCount int64
	electionTimer *time.Timer 

	// voteCount
	voteCount int
	totalCount int
	vcLock sync.Mutex




}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentState.Load() == LEADER
	rf.mu.Unlock()
	// if isleader {
	// 	DPrintf("[Server %d, Term %d] is leader", rf.me, rf.currentTerm)
	// } else {
	// 	DPrintf("[Server %d, Term %d] is not leader", rf.me, rf.currentTerm)
	// }
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
	// Your code here (2C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Server int
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. Reply false if term < currentTerm (§5.1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Server = rf.me
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		originalState := rf.currentState.Load()
		rf.currentState.Store(FOLLOWER)
		if originalState == LEADER {
			rf.notLeaderCh <- true
		}
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentState.Store(FOLLOWER)
			// reset election timer
		rf.resetElectionTimer()	
		return
	}


}

// return if the prevLogIndex and prevLogTerm is at least as up-to-date as receiver's log
func (rf *Raft) isUpToDate(prevLogIndex int, prevLogTerm int) bool {
	// 1. If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// 2. If the logs end with the same term, then whichever log is longer is more up-to-date.
	// 3. If the logs are the same length and end with the same term, then the logs are up-to-date.
	if prevLogTerm == -1 && prevLogIndex == -1 && len(rf.log) == 0 {
		return true
	}
	if prevLogTerm > rf.log[len(rf.log)-1].Term {
		return true
	} else if prevLogTerm == rf.log[len(rf.log)-1].Term {
		if prevLogIndex >= len(rf.log) {
			return true
		}
	}
	return false
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
	if reply.Term != args.Term {
		return ok
	}

	if ok {
		rf.voteCh <- *reply
	}
	return ok
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}
// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term int
	Success bool
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 1. Reply false if term < currentTerm (§5.1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		originalState := rf.currentState.Load()
		rf.currentState.Store(FOLLOWER)
		if originalState == LEADER {
			rf.notLeaderCh <- true
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	// DPrintf("Server %d receives heartbeat from server %d in term %d", rf.me, args.LeaderId, args.Term)
	// currently no check for log consistency
	// reset election timer
	rf.resetElectionTimer()
	return

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if reply.Term != args.Term {
		return ok
	}
	// Leader Won't Commit Entries From Previous Terms
	rf.mu.Lock()
	curTerm := rf.currentTerm
	rf.mu.Unlock()
	if curTerm != args.Term {
		return ok
	}

	// DPrintf("[Server %d, Term %d] receives heartbeat reply from server %d", rf.me, curTerm, server)
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

	// Your code here (2B).


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
func (rf *Raft) election(electionProc int) {
	close(rf.voteCh)
	rf.mu.Lock()
	rf.voteCh = make(chan RequestVoteReply)
	rf.currentTerm++; 
	rf.votedFor = rf.me
	rf.currentState.Store(CANDIDATE)
	// send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			
			args := RequestVoteArgs{rf.currentTerm, rf.me, -1, -1} // temp implementation
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
	rf.mu.Unlock()
	//count the votes
	voteCount := 1
	totalCount := 1
	ch := rf.voteCh
	for totalCount < len(rf.peers) {
		rf.mu.Lock()
		curTerm := rf.currentTerm
		rf.mu.Unlock()
		reply, ok := <- ch
		if !ok {
			DPrintf("[Server %d, Term %d] proc %d channel closed", rf.me, curTerm, electionProc)
			return
		}
		DPrintf("[Server %d, Term %d] receives vote %v from [Server %d, Term %d], %d/%d/%d", rf.me, curTerm, reply.VoteGranted, reply.Server, reply.Term, voteCount, totalCount, len(rf.peers))
		if reply.Term != curTerm {
			continue
		}

		totalCount++
		if reply.VoteGranted {
			voteCount++
		}
		if voteCount > len(rf.peers) / 2 && rf.currentState.Load() != LEADER {
			// become leader
			rf.currentState.Store(LEADER)
			go rf.heartbeats()
			rf.isLeaderCh <- electionProc
		}
	
	}


} 

func (rf *Raft) resetElectionTimer() {
	// randomize the election timeout
	timeout := 400 + rand.Int63() % 300
	rf.electionTimer.Reset(time.Duration(timeout) * time.Millisecond)
}

func (rf *Raft) ticker() {
	rf.timeCount = 0
	// DPrintf("[Server %d, Term %d] resets election timer", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()
	proc := 0
	for {
		if rf.currentState.Load() != LEADER {
			select {
			case <- rf.electionTimer.C:
				// start election
				rf.mu.Lock()
				DPrintf("[Server %d, Term %d] starts election proc %d", rf.me, rf.currentTerm, proc)
				rf.mu.Unlock()
				// sign and cond are used to detect timeout
				go rf.election(proc)
				proc++
				rf.mu.Lock()
				rf.resetElectionTimer()
				rf.mu.Unlock()

			case proc := <- rf.isLeaderCh:
				rf.mu.Lock()
				DPrintf("[server %d, term %d] proc %d is leader and wait", rf.me, rf.currentTerm, proc)
				rf.mu.Unlock()
			}
		} else {

			<- rf.notLeaderCh
		}
	}
}



// send heartbeats to all peers regularly
func (rf *Raft) heartbeats() {
	// temp implementation only send empty heartbeats
	// send heartbeats to all peers regularly
	for {
		// DPrintf("[Server %d, Term %d] sends heartbeat", rf.me, rf.currentTerm)
		if rf.currentState.Load() != LEADER {
			return
		}
		rf.mu.Lock()
		for i := 0; i < len(rf.peers); i++ {
				
			if i != rf.me {
				args := AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, nil, -1}
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(120) * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	// need re-initialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.currentState.Store(FOLLOWER)
	// Channels
	rf.voteCh = make(chan RequestVoteReply)
	rf.notLeaderCh = make(chan bool)
	rf.isLeaderCh = make(chan int)

	rf.electionTimer = time.NewTimer(time.Duration(400) * time.Millisecond)
	rf.resetElectionTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
