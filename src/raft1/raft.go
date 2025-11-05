package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *tester.Persister   // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int
	votedFor    int
	log         []any
	state       string
	votes       int

	applyChannel chan raftapi.ApplyMsg
	hbeat        chan bool
	winElec      chan bool
	stepDown     chan bool
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == "leader"
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
	CandidateId int
	Term        int
	// LastLogIndex int
	// LastLogTerm  int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// PrevLogIndex int
	// PreLogTerm   int
	// Entries      []any
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.currentTerm = args.Term
		if rf.currentTerm < args.Term {
			rf.votedFor = -1
			rf.votes = 0
			rf.state = "follower"
			rf.mu.Unlock()
			select {
			case rf.stepDown <- true:
			default:
			}
			rf.mu.Lock()
		}
		reply.Term = args.Term
		reply.Success = true
	}
	rf.mu.Unlock()
	select {
	case rf.hbeat <- true:
	default:
	}
	// for 3A only implemented till heartbeat
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	fmt.Println(ok)
}
func (rf *Raft) broadcastAppendEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		reply := AppendEntriesReply{}

		go rf.sendAppendEntry(i, &args, &reply)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.votes = 0
		rf.state = "follower"
		rf.mu.Unlock()
		select {
		case rf.stepDown <- true:
		default:
		}
		return
	}
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
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
func (rf *Raft) handleVoting(server int) {
	rf.mu.Lock()
	args := RequestVoteArgs{CandidateId: rf.me, Term: rf.currentTerm}
	reply := RequestVoteReply{}
	if rf.state != "candidate" {
		fmt.Println("not a candidate")
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	ok := rf.sendRequestVote(server, &args, &reply)
	rf.mu.Lock()

	if ok && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.votes = 0
		rf.state = "follower"
		rf.mu.Unlock()
		select {
		case rf.stepDown <- true:
		default:
		}
		return
	}
	if ok && reply.VoteGranted {
		rf.votes++
	} else if ok && !reply.VoteGranted {
		rf.currentTerm = reply.Term
	}
	l := len(rf.peers)
	if rf.votes >= (l/2)+1 {
		rf.mu.Unlock()
		select {
		case rf.winElec <- true:
		default:
		}
		return
	}
	rf.mu.Unlock()

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votes = 0
	rf.state = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votes++
	for i := range rf.peers {
		if i != rf.me {
			go rf.handleVoting(i)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case "follower":
			select {
			case <-rf.hbeat:
			case <-time.After(time.Duration(50+(rand.Int63()%350)) * time.Millisecond):
				rf.startElection()
			}
		case "candidate":
			select {
			case <-rf.winElec:
				rf.mu.Lock()
				rf.votedFor = -1
				rf.votes = 0
				rf.state = "leader"
				rf.mu.Unlock()
				rf.broadcastAppendEntry()
			case <-rf.stepDown:
			case <-time.After(time.Duration(50+(rand.Int63()%350)) * time.Millisecond):
				rf.startElection()
			}
		case "leader":
			select {
			case <-rf.stepDown:
			case <-time.After(time.Duration(150) * time.Millisecond):
				rf.broadcastAppendEntry()
			}

		}
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
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.state = "follower"
	rf.applyChannel = applyCh
	rf.hbeat = make(chan bool, 1)
	rf.winElec = make(chan bool, 1)
	rf.stepDown = make(chan bool, 1)
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
