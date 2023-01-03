package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut	   = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string
	MsgId    	int64
	ClientId 	int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	MsgId    	int64
	ClientId 	int64
}

type GetReply struct {
	Err   Err
	Value string
}
