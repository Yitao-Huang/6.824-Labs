package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const (
	PullConfigInterval       = time.Millisecond * 100
	PullShardsInterval       = time.Millisecond * 200
	WaitCmdTimeOut           = time.Millisecond * 500
	ReqCleanShardDataTimeOut = time.Millisecond * 500
)

type Op struct {
	Key       string
	Value     string
	Op        string
	ClientId  int64
	MsgId     int64
	ReqId     int64
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config 		  shardmaster.Config
	oldConfig 	  shardmaster.Config
	notifyCh	  map[int64]chan NotifyMsg
	ids   		  [shardmaster.NShards]map[int64]int64 // clientId -> msgId map
	ownShards     map[int]bool
	data 		  [shardmaster.NShards]map[string]string
	waitShardIds  map[int]bool
	historyShards map[int]map[int]MergeShardData
	mck           *shardmaster.Clerk

	dead 			int32
	killCh       	chan struct{}
	persister      	*raft.Persister
	lastApplyIndex  int
	lastApplyTerm   int

	pullConfigTimer *time.Timer
	pullShardsTimer *time.Timer
}

func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.killCh:
			return
		case <-kv.pullConfigTimer.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullConfigTimer.Reset(PullConfigInterval)
				break
			}

			kv.mu.Lock()
			lastNum := kv.config.Num
			kv.mu.Unlock()

			config := kv.mck.Query(lastNum + 1)
			if config.Num == lastNum+1 {
				kv.mu.Lock()
				if len(kv.waitShardIds) == 0 && kv.config.Num+1 == config.Num {
					kv.mu.Unlock()
					kv.rf.Start(config.Copy())
				} else {
					kv.mu.Unlock()
				}
			}
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.killCh)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) duplicateFilter(shardId int, clientId int64, id int64) bool {
	if val, ok := kv.ids[shardId][clientId]; ok {
		return val == id
	}
	return false
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.persister = persister

	kv.mck = shardmaster.MakeClerk(kv.masters)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killCh = make(chan struct{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = [shardmaster.NShards]map[string]string{}
	for i, _ := range kv.data {
		kv.data[i] = make(map[string]string)
	}
	kv.ids = [shardmaster.NShards]map[int64]int64{}
	for i, _ := range kv.ids {
		kv.ids[i] = make(map[int64]int64)
	}

	kv.waitShardIds = make(map[int]bool)
	kv.historyShards = make(map[int]map[int]MergeShardData)

	config := shardmaster.Config{
		Num:    0,
		Shards: [shardmaster.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.config = config
	kv.oldConfig = config
	kv.readSnapShotData(kv.persister.ReadSnapshot())

	kv.notifyCh = make(map[int64]chan NotifyMsg)
	kv.pullConfigTimer = time.NewTimer(PullConfigInterval)
	kv.pullShardsTimer = time.NewTimer(PullShardsInterval)

	go kv.waitApplyCh()
	go kv.pullConfig()
	go kv.pullShards()

	return kv
}

func (kv *ShardKV) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	// need snapshot
	data := kv.genSnapshotData()
	kv.rf.TakeSnapshot(logIndex, data)
}

func (kv *ShardKV) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.data) != nil ||
		e.Encode(kv.ids) != nil ||
		e.Encode(kv.waitShardIds) != nil ||
		e.Encode(kv.historyShards) != nil ||
		e.Encode(kv.config) != nil ||
		e.Encode(kv.oldConfig) != nil ||
		e.Encode(kv.ownShards) != nil {
		panic("gen snapshot data encode err")
	}

	data := w.Bytes()
	return data
}

func (kv *ShardKV) readSnapShotData(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData [shardmaster.NShards]map[string]string
	var ids [shardmaster.NShards]map[int64]int64
	var waitShardIds map[int]bool
	var historyShards map[int]map[int]MergeShardData
	var config shardmaster.Config
	var oldConfig shardmaster.Config
	var ownShards map[int]bool

	if d.Decode(&kvData) != nil ||
		d.Decode(&ids) != nil ||
		d.Decode(&waitShardIds) != nil ||
		d.Decode(&historyShards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&oldConfig) != nil ||
		d.Decode(&ownShards) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.data = kvData
		kv.ids = ids
		kv.waitShardIds = waitShardIds
		kv.historyShards = historyShards
		kv.config = config
		kv.oldConfig = oldConfig
		kv.ownShards = ownShards
	}
}

func (kv *ShardKV) configReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.config.Num {
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	if _, ok := kv.ownShards[shardId]; !ok {
		return ErrWrongGroup
	}
	if _, ok := kv.waitShardIds[shardId]; ok {
		return ErrWrongGroup
	}
	return OK
}
