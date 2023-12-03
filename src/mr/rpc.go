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



// Add your RPC definitions here.
type HeartBeatArgs struct {
	WorkerId string
	Status int
}

// RPC arguments for worker to send map complete message to coordinator
type MapCompleteArgs struct {
	WorkerId string
	MapId int
	IFilePath string
}

// RPC arguments for worker to send reduce complete message to coordinator
type ReduceCompleteArgs struct {
	WorkerId string
	ReduceId int
	OFilePath string
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId string
}


type EmptyArgs struct {
}


type TaskType int
const (
	Map_t TaskType = iota
	Reduce_t
	Stop_t
	Empty_t
)

type MrTask struct {
	FilePath string
	MrType TaskType 
	MapId int
	ReduceId int
}


type TaskReply struct {
	Task MrTask
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
