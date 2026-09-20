package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type taskType int

const (
	mapTask taskType = iota
	reduceTask
)

type TaskObject struct {
	TaskId       int64
	MapId        int
	ReduceId     int
	NReduce      int
	TaskType     taskType
	MapInput     string
	ReduceInputs []string
}

type RegisterArgs struct {
	WorkerId int64
}

type RegisterReply struct {
	Success bool
}

type GetTaskArgs struct {
	WorkerId int64
}

type GetTaskReply struct {
	IsWorkAvailable bool
	TaskObject      TaskObject
}

type DoneTaskArgs struct {
	WorkerId     int64
	TaskId       int64
	IsSuccessful bool
	MapOutputs   []string
	ReduceOutput string
}

type DoneTaskReply struct {
	IsWorkAvailable bool
}
