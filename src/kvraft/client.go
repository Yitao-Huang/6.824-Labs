package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const (
	ChangeLeaderInterval = time.Millisecond * 20
)

type Clerk struct {
	servers  	[]*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId 	int64
	leaderId 	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, MsgId: nrand(), ClientId: ck.clientId}
	leaderId := ck.leaderId

	for {
		reply := GetReply{}
		// log.Printf("client Get %+v", args)

		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)

		if !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return reply.Value
		case ErrNoKey:
			ck.leaderId = leaderId
			return ""
		case ErrTimeOut:
			continue
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		MsgId:    nrand(),
		ClientId: ck.clientId,
	}

	leaderId := ck.leaderId
	for {
		reply := PutAppendReply{}
		// log.Printf("client PutAppend %+v", args)

		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return
		case ErrTimeOut:
			continue
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
