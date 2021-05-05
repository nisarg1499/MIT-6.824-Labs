package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Stages of task
// 0 not yet started, 1 started, 2 completed

type Master struct {
	// Your definitions here.
	mapTasks            []MapTask
	reduceTasks         []ReduceTask
	mu                  sync.Mutex
	numberOfReduceTasks int
}

type MapTask struct {
	taskId   int
	stage    int
	fileName string
}

type ReduceTask struct {
	taskId       int
	stage        int
	bucketNumber int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetWorkerTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Println("New task request from worker")

	if !m.checkIfAllMapTasksAreDone() {
		fmt.Println("Inside map tasks allotment, before for loop")
		// if all reduce jobs are not completed
		for i, mt := range m.mapTasks {
			fmt.Println("Iterating map tasks")
			if mt.stage == 0 {
				fmt.Println("In stage == 0")
				// adding all values im reply for a task
				reply.TaskId = mt.taskId
				reply.FileName = mt.fileName
				reply.TaskType = "Map"
				reply.AllTasksDone = false
				reply.NumberOfReducers = m.numberOfReduceTasks

				// run that task
				mt.stage = 1
				m.mapTasks[i] = mt

				fmt.Printf("Map task started running %+v\n", mt)
				return nil
			}
		}
	} else if !m.checkIfAllReduceTasksAreDone() {
		// if all reduce jobs are not completed
		for i, rt := range m.reduceTasks {
			if rt.stage == 0 {
				reply.TaskId = rt.taskId
				reply.TaskType = "Reduce"
				reply.AllTasksDone = false
				reply.NumberOfReducers = m.numberOfReduceTasks
				reply.BucketNumber = i

				rt.stage = 1
				m.reduceTasks[i] = rt
				fmt.Printf("Reduce task started running %+v\n", rt)

				return nil
			}
		}
	} else {
		// if all tasks are done
		reply.AllTasksDone = true
		return nil
	}
	return nil
}

func (m *Master) checkIfAllMapTasksAreDone() bool {

	for _, mt := range m.mapTasks {
		if mt.stage != 2 {
			return false
		}
	}
	return true
}

func (m *Master) checkIfAllReduceTasksAreDone() bool {
	for _, rt := range m.reduceTasks {
		if rt.stage != 2 {
			return false
		}
	}
	return true
}

func (m *Master) ReportOnMap(args *ReportOnMapToMasterArgs, reply *ReportOnMapToMasterReply) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Status == 2 {
		m.mapTasks[args.TaskId].stage = 2
	} else {
		m.mapTasks[args.TaskId].stage = 0
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.numberOfReduceTasks = nReduce

	// initialize map tasks
	m.mapTasks = make([]MapTask, len(files))

	for i, f := range files {
		m.mapTasks[i] = MapTask{taskId: i, stage: 0, fileName: f}
	}

	// initialize reuduce tasks
	m.reduceTasks = make([]ReduceTask, nReduce)

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = ReduceTask{taskId: i, stage: 0, bucketNumber: i}
	}
	m.server()
	return &m
}
