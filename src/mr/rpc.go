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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetReduceArgs struct {
}

type GetReduceReply struct {
	NReduce int
}

type GetTaskArgs struct {
	Workerid int
}

type GetTaskReply struct {
	Id       int
	File     string
	TaskType string
}

type ReportDoneArgs struct {
	Workerid int
	TaskType string
	Id       int
}

type ReportDoneReply struct {
	Succ bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
