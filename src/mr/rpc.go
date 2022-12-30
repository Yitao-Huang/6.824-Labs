package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type RegisterArgs struct {
}

type RegisterReply struct {
	WID			int
}

type GetTaskArgs struct {
	WID			int
}

type GetTaskReply struct {
	Task 		Task
}

type FinishTaskArgs struct {
	TaskNum		int
	Type		string
	WID			int
	Done 		bool
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
