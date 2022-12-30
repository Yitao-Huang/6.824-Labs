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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"

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

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

const (
	ElectionTimeout 	= 150 * time.Millisecond
	HeartBeatTimeout 	= 150 * time.Millisecond
	RPCTimeout 			= 100 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]
	dead      	int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor	int
	logs		[]LogEntry

	role		Role
	applyCh		chan ApplyMsg

	commitIndex	int
	lastApplied int

	nextIndex	[]int
	matchIndex	[]int

	electionTimer *time.Timer
	appendEntriesTimers []*time.Timer

	lastSnapshotIndex int
	lastSnapshotTerm  int
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.logs[len(rf.logs)-1].Term
	index := rf.lastSnapshotIndex + len(rf.logs) - 1
	return term, index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.role == Leader
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
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var voteFor int
	var logs []LogEntry
	var commitIndex, lastSnapshotIndex, lastSnapshotTerm int

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("rf read persist err")
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.commitIndex = commitIndex
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.logs = logs
	}

}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
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

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex 	int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int
	Success			bool
	NextIndex		int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	log.Printf("Server %d received RequestVote RPC %+v, current state: %+v", rf.me, args, rf)

	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm {
		if rf.role == Leader {
			return
		}
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			return
		}
		// not voted
	}

	defer rf.persist()

	LastLogIndex, LastLogTerm := rf.lastLogTermIndex()
	// update term if see a larger term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	if LastLogTerm > args.LastLogTerm || (LastLogTerm == args.LastLogTerm && LastLogIndex > args.LastLogIndex) {
		return
	}

	rf.currentTerm = args.Term
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.role = Follower

	log.Printf("Server %d granted vote for %d, term: %d", rf.me, args.CandidateId, args.Term)

	rf.resetElectionTimer()
}

func randomElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomElectionTimeout())
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	if rf.role == Leader {
		rf.mu.Unlock()
		return
	}

	log.Printf("Server %d start election", rf.me)

	// switch to candidate
	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	// set RequestVoteArgs
	var args RequestVoteArgs
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex, args.LastLogTerm = rf.lastLogTermIndex()

	rf.persist()

	rf.mu.Unlock()

	grantedCount := 1
	chResCount := 1
	votesCh := make(chan bool, len(rf.peers))

	// send RequestVote to all peers
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(ch chan bool, index int) {
			var reply RequestVoteReply

			rf.sendRequestVote(index, &args, &reply)
			ch <- reply.VoteGranted 

			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.persist()
				}
				rf.mu.Unlock()
			}

		}(votesCh, idx)
	}

	for {
		r := <-votesCh
		chResCount += 1
		if r {
			grantedCount += 1
		}
		// get all peer response OR get majority voteGranted OR get majority no voteGranted
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || chResCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	// don't have majority vote, election fail
	if grantedCount <= len(rf.peers)/2 {
		return
	}

	rf.mu.Lock()
	if rf.currentTerm == args.Term && rf.role == Candidate {
		rf.role = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		_, lastLogIndex := rf.lastLogTermIndex()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
		rf.resetElectionTimer()
		rf.persist()
	}
	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}
	rf.mu.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)

		ch := make(chan bool, 1)
		r := RequestVoteReply{}
		log.Printf("Server %d send RequestVote to %d", rf.me, server)

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			ch <- ok
		}()

		select {
		case <-t.C:
			return
		case <-rpcTimer.C:
			continue
		case ok := <-ch:
			if ok {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return
			} else {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}


func (rf *Raft) resetHeartBeatTimers() {
	for i, _ := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(0)
	}
}

func (rf *Raft) resetHeartBeatTimer(peer int) {
	rf.appendEntriesTimers[peer].Stop()
	rf.appendEntriesTimers[peer].Reset(HeartBeatTimeout)
}

func (rf *Raft) getAppendEntriesArgs(peer int) AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
	}
	return args
}

func (rf *Raft) appendEntriesToPeer(peer int) {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.mu.Lock()

		rf.resetHeartBeatTimer(peer)
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		args := rf.getAppendEntriesArgs(peer)

		rf.mu.Unlock()

		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)

		var reply AppendEntriesReply
		ch := make(chan bool, 1)

		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
			ch <- ok
		}(&args, &reply)

		select {
		case <-RPCTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			}
		}

		rf.mu.Lock()

		if rf.currentTerm < reply.Term {
			rf.role = Follower
			rf.resetElectionTimer()
			rf.currentTerm = reply.Term
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			if reply.NextIndex > rf.nextIndex[peer] {
				rf.nextIndex[peer] = reply.NextIndex
				rf.matchIndex[peer] = reply.NextIndex - 1
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	rf.role = Follower
	rf.resetElectionTimer()
	rf.persist()
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.role == Leader
	_, index := rf.lastLogTermIndex()
	index++

	if isLeader {
		rf.logs = append(rf.logs, LogEntry{
			Term: rf.currentTerm,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	rf.resetHeartBeatTimers()

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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.role = Follower
	rf.logs = make([]LogEntry, 1)

	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i, _ := range rf.peers {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatTimeout)
	}

	go func(){
		for !rf.killed() {
			<-rf.electionTimer.C
			rf.startElection()
		}
	}()

	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			for !rf.killed() {
				<-rf.appendEntriesTimers[index].C
				rf.appendEntriesToPeer(index)
			}
		}(i)
	}
	
	return rf
}
