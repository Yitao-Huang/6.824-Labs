package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex 	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int
	Success			bool
	NextIndex		int
}


func (rf *Raft) getNextIndex() int {
	_, idx := rf.lastLogTermIndex()
	return idx + 1
}

func (rf *Raft) AppendEntriesOutOfOrder(args *AppendEntriesArgs) bool {
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	lastTerm, lastIndex := rf.lastLogTermIndex()
	if argsLastIndex < lastIndex && lastTerm == args.Term {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	_, lastLogIndex := rf.lastLogTermIndex()

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.Success = false
		reply.NextIndex = rf.lastSnapshotIndex + 1

	} else if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.NextIndex = rf.getNextIndex()

	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		if rf.AppendEntriesOutOfOrder(args) {
			reply.Success = false
			reply.NextIndex = 0

		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}

	} else if args.PrevLogTerm == rf.logs[rf.getIdxWithSnapshot(args.PrevLogIndex)].Term {
		if rf.AppendEntriesOutOfOrder(args) {
			reply.Success = false
			reply.NextIndex = 0

		} else {
			reply.Success = true
			rf.logs = append(rf.logs[0:rf.getIdxWithSnapshot(args.PrevLogIndex)+1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}

	} else {
		reply.Success = false

		term := rf.logs[rf.getIdxWithSnapshot(args.PrevLogIndex)].Term
		idx := args.PrevLogIndex

		for idx > rf.commitIndex && idx > rf.lastSnapshotIndex && rf.logs[rf.getIdxWithSnapshot(idx)].Term == term {
			idx -= 1
		}

		reply.NextIndex = idx + 1
	}

	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			// log.Printf("%d commit %d", rf.me, args.LeaderCommit)
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	// log.Printf("%d received AppendEntries from %d, args: %+v, reply: %+v", rf.me, args.LeaderId, args, reply)

	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) getAppendEntriesArgs(peer int) AppendEntriesArgs {
	nextIndex := rf.nextIndex[peer]
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	// no log to send
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		args.PrevLogIndex = lastLogIndex
		args.PrevLogTerm = lastLogTerm
		return args
	}

	args.Entries = append([]LogEntry{}, rf.logs[rf.getIdxWithSnapshot(nextIndex):]...)
	args.PrevLogIndex = nextIndex - 1

	if args.PrevLogIndex == rf.lastSnapshotIndex {
		args.PrevLogTerm = rf.lastSnapshotTerm
	} else {
		args.PrevLogTerm = rf.getLogByIndex(args.PrevLogIndex).Term
	}

	return args
}

func (rf *Raft) resetHeartBeatTimers() {
	for i := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(0)
	}
}

func (rf *Raft) resetHeartBeatTimer(peer int) {
	rf.appendEntriesTimers[peer].Stop()
	rf.appendEntriesTimers[peer].Reset(HeartBeatTimeout)
}

func (rf *Raft) appendEntriesToPeer(peer int) {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.resetHeartBeatTimer(peer)
			rf.mu.Unlock()
			return
		}
		// log.Printf("%d sending AppendEntries to %d", rf.me, peer)
		args := rf.getAppendEntriesArgs(peer)
		rf.resetHeartBeatTimer(peer)
		rf.mu.Unlock()

		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)

		reply := AppendEntriesReply{}
		ch := make(chan bool, 1)

		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}(&args, &reply)

		select {
		case <-rf.killCh:
			return
		case <-RPCTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			}
		}

		rf.mu.Lock()
		// log.Printf("%d sent AppendEntries to %d, reply: %+v", rf.me, peer, reply)
		if reply.Term > rf.currentTerm {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.currentTerm = reply.Term
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if rf.role != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			if reply.NextIndex >= rf.nextIndex[peer] {
				rf.nextIndex[peer] = reply.NextIndex
				rf.matchIndex[peer] = reply.NextIndex - 1
			}

			// should only commit entry in current term
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
				rf.updateCommitIndex()
			}

			rf.persist()
			rf.mu.Unlock()
			return
		}

		// on failure
		if reply.NextIndex != 0 {
			if reply.NextIndex > rf.lastSnapshotIndex {
				rf.nextIndex[peer] = reply.NextIndex
				rf.mu.Unlock()
				// retry
				continue
			} else {
				go rf.sendInstallSnapshot(peer)
				rf.mu.Unlock()
				return
			}
		} else {
			// random order
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	commit := false
	for i := rf.commitIndex + 1; i <= rf.lastSnapshotIndex+len(rf.logs); i++ {
		count := 0
		for _, idx := range rf.matchIndex {
			if idx >= i {
				count++
				if count > len(rf.peers)/2 {
					// log.Printf("%d commit %d", rf.me, i)
					rf.commitIndex = i
					commit = true
					break
				}
			}
		}

		if rf.commitIndex != i {
			break
		}
	}

	if commit {
		rf.notifyApplyCh <- struct{}{}
	}
}
