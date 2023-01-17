package shardmaster

import (
	"sort"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const WaitCmdTimeOut = 100 * time.Millisecond

type NotifyMsg struct {
	Err         Err
	WrongLeader bool
	Config      Config
}

type ShardMaster struct {
	mu      	sync.Mutex
	me      	int
	rf      	*raft.Raft
	applyCh 	chan raft.ApplyMsg
	killCh  	chan struct{}

	// Your data here.
	msgNotify 	map[int64]chan NotifyMsg
	ids 		map[int64]int64

	configs 	[]Config // indexed by config num
}


type Op struct {
	MsgId    int64
	ReqId    int64
	ClientId int64
	Args 	 interface{}
	Method   string
}

func (sm *ShardMaster) runCmd(method string, id int64, clientId int64, args interface{}) (res NotifyMsg) {
	op := Op{
		MsgId:    id,
		ReqId:    nrand(),
		Args:     args,
		Method:   method,
		ClientId: clientId,
	}
	res = sm.waitCmd(op)
	return
}

func (sm *ShardMaster) waitCmd(op Op) (res NotifyMsg) {
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		res.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	sm.msgNotify[op.ReqId] = ch
	sm.mu.Unlock()

	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()

	select {
	case res = <-ch:
		sm.removeCh(op.ReqId)
		return
	case <-t.C:
		sm.removeCh(op.ReqId)
		res.WrongLeader = true
		res.Err = ErrTimeout
		return
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	res := sm.runCmd("Join", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	res := sm.runCmd("Leave", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	res := sm.runCmd("Move", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sm.mu.Lock()
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sm.getConfig(args.Num)
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	res := sm.runCmd("Query", args.MsgId, args.ClientId, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}

func (sm *ShardMaster) getConfig(num int) Config {
	if num < 0 || num >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1].Copy()
	} else {
		return sm.configs[num].Copy()
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.killCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) removeCh(id int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.msgNotify, id)
}

func (sm *ShardMaster) duplicateFilter(clientId int64, id int64) bool {
	if val, ok := sm.ids[clientId]; ok {
		return val == id
	}
	return false
}

func (sm *ShardMaster) applyMsg() {
	for {
		select {
		case <-sm.killCh:
			return
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)

			sm.mu.Lock()
			isRepeated := sm.duplicateFilter(op.ClientId, op.MsgId)
			if !isRepeated {
				switch op.Method {
				case "Join":
					sm.doJoin(op.Args.(JoinArgs))
				case "Leave":
					sm.doLeave(op.Args.(LeaveArgs))
				case "Move":
					sm.doMove(op.Args.(MoveArgs))
				case "Query":
				default:
					panic("unknown method")
				}
			}

			res := NotifyMsg {
				Err:         OK,
				WrongLeader: false,
			}

			if op.Method != "Query" {
				sm.ids[op.ClientId] = op.MsgId
			} else {
				res.Config = sm.getConfig(op.Args.(QueryArgs).Num)
			}
			if ch, ok := sm.msgNotify[op.ReqId]; ok {
				ch <- res
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) doJoin(args JoinArgs) {
	config := sm.getConfig(-1)
	config.Num += 1

	for k, v := range args.Servers {
		config.Groups[k] = v
	}

	sm.rebalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) doLeave(args LeaveArgs) {
	config := sm.getConfig(-1)
	config.Num += 1

	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		for i, v := range config.Shards {
			if v == gid {
				config.Shards[i] = 0
			}
		}
	}

	sm.rebalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) doMove(args MoveArgs) {
	config := sm.getConfig(-1)
	config.Num += 1
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) rebalance(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}

	} else if len(config.Groups) == 1 {
		for k, _ := range config.Groups {
			for i, _ := range config.Shards {
				config.Shards[i] = k
			}
		}

	} else if len(config.Groups) <= NShards {
		avg := NShards / len(config.Groups)
		rest := NShards - avg*len(config.Groups)
		finish := true
		lastGid := 0

	LOOP:
		var gids []int
		for k := range config.Groups {
			gids = append(gids, k)
		}
		sort.Ints(gids)

		for _, gid := range gids {
			lastGid = gid
			count := 0

			for _, val := range config.Shards {
				if val == gid {
					count += 1
				}
			}
			
			if count == avg {
				continue
			} else if count > avg && rest == 0 {
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg {
							config.Shards[i] = 0
						} else {
							c += 1
						}
					}
				}

			} else if count > avg && rest > 0 {
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg+rest {
							config.Shards[i] = 0
						} else {
							if c == avg {
								rest -= 1
							} else {
								c += 1
							}
						}
					}
				}

			} else {
				for i, val := range config.Shards {
					if count == avg {
						break
					}
					if val == 0 && count < avg {
						config.Shards[i] = gid
						count++
					}
				}

				if count < avg {
					finish = false
				}
			}
		}

		if !finish {
			finish = true
			goto LOOP
		}

		if lastGid != 0 {
			for i, val := range config.Shards {
				if val == 0 {
					config.Shards[i] = lastGid
				}
			}
		}

	} else {
		gids := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for i, gid := range config.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, i)
				continue
			}
			if _, ok := gids[gid]; ok {
				emptyShards = append(emptyShards, i)
				config.Shards[i] = 0
			} else {
				gids[gid] = 1 // placeholder
			}
		}

		n := 0
		if len(emptyShards) > 0 {
			var keys []int
			for k := range config.Groups {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, gid := range keys {
				if _, ok := gids[gid]; !ok {
					config.Shards[emptyShards[n]] = gid
					n += 1
				}
				if n >= len(emptyShards) {
					break
				}
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.killCh = make(chan struct{})
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.ids = make(map[int64]int64)
	sm.msgNotify = make(map[int64]chan NotifyMsg)

	go sm.applyMsg()
	return sm
}
