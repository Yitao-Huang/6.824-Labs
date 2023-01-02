package raft

import (
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// log.Printf("Server %d received RequestVote RPC %+v", rf.me, args)

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
	// update term if see a larger term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeRole(Follower)
	}

	if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	// log.Printf("Server %d granted vote for %d, term: %d", rf.me, args.CandidateId, args.Term)
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomElectionTimeout())
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for {
		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)

		ch := make(chan bool, 1)
		// log.Printf("Server %d send RequestVote to %d", rf.me, server)

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				time.Sleep(10 * time.Millisecond)
			}
			ch <- ok
		}()

		select {
		case <-t.C:
			return
		case <-RPCTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				return
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.electionTimer.Reset(randomElectionTimeout())
	if rf.role == Leader {
		rf.mu.Unlock()
		return
	}

	// log.Printf("Server %d start election", rf.me)
	// switch to candidate
	rf.changeRole(Candidate)

	// set RequestVoteArgs
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
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
			reply := RequestVoteReply{}

			rf.sendRequestVote(index, &args, &reply)
			ch <- reply.VoteGranted 

			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
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
		// log.Printf("%d become the new leader, commitIndex: %d", rf.me, rf.commitIndex)
		rf.changeRole(Leader)
		rf.persist()
	}
	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}
	rf.mu.Unlock()
}
