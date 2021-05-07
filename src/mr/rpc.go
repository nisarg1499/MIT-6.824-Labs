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

type ReportOnMapToMasterArgs struct {
	Status               int
	TaskId               int
	TempMapFilesLocation []string
}

type ReportOnMapToMasterReply struct {
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskId               int
	FileName             string
	TaskType             string
	AllTasksDone         bool
	NumberOfReducers     int
	TempMapFilesLocation []string
}

// Would need a couple of definitions for map and reduce tasks

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
