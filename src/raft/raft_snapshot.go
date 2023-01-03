package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(peer int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	// log.Printf("%d sent InstallSnapshot to %d, term %d", args.LeaderId, peer, args.Term)
	rf.mu.Unlock()

	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for {
		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)

		ch := make(chan bool, 1)
		reply := InstallSnapshotReply{}

		go func() {
			ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(10 * time.Millisecond)
			}
			ch <- ok
		}()
	    
		ok := false
		select {
		case <-rf.killCh:
			return
		case <-RPCTimer.C:
			continue
		case ok = <-ch:
			if !ok {
				continue
			}
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.role != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}

		if args.LastIncludedIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
		}

		return
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("%d received InstallSnapshot from %d, term %d", rf.me, args.LeaderId, args.Term)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.currentTerm = args.Term
		rf.changeRole(Follower)
		rf.resetElectionTimer()
		defer rf.persist()
	}

	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		return
	}

	start := args.LastIncludedIndex - rf.lastSnapshotIndex
	if start >= len(rf.logs) {
		rf.logs = make([]LogEntry, 1)
		rf.logs[0].Term = args.LastIncludedTerm
	} else {
		rf.logs = rf.logs[start:]
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
}

func (rf *Raft) TakeSnapshot(logIndex int, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// less than last snapshot
	if logIndex <= rf.lastSnapshotIndex {
		return
	}

	// update snapshot
	lastLogIncluded := rf.getLogByIndex(logIndex)
	rf.logs = rf.logs[rf.getIdxWithSnapshot(logIndex):]
	rf.lastSnapshotIndex = logIndex
	rf.lastSnapshotTerm = lastLogIncluded.Term

	// persist state
	persistData := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(persistData, snapshotData)
}