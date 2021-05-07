package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// Stages of task
// 0 not yet started, 1 started, 2 completed

type Master struct {
	// Your definitions here.
	mapTasks             []MapTask
	reduceTasks          []ReduceTask
	mu                   sync.Mutex
	numberOfReduceTasks  int
	tempMapFilesLocation [][]string
}

type MapTask struct {
	taskId   int
	stage    int
	fileName string
}

type ReduceTask struct {
	taskId int
	stage  int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetWorkerTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// fmt.Println("New task request from worker")

	if !m.checkIfAllMapTasksAreDone() {
		// fmt.Println("Inside map tasks allotment, before for loop")
		// if all reduce jobs are not completed
		for i, mt := range m.mapTasks {
			// fmt.Println("Iterating map tasks")
			if mt.stage == 0 {
				// fmt.Printf("Assigning task with taskId %d\n", mt.taskId)
				// adding all values im reply for a task
				reply.TaskId = mt.taskId
				reply.FileName = mt.fileName
				reply.TaskType = "Map"
				reply.AllTasksDone = false
				reply.NumberOfReducers = m.numberOfReduceTasks

				// run that task
				mt.stage = 1
				m.mapTasks[i] = mt

				go TenSecondsCheckMap(mt.taskId, m)
				// fmt.Printf("Map task started running %+v\n", mt)
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
				temporaryFileNames := make([]string, 0)
				// Fetched the arr row[], and then appending small chunks name into temporaryFileNames
				for _, arr := range m.tempMapFilesLocation {
					temporaryFileNames = append(temporaryFileNames, arr[rt.taskId])
				}
				reply.TempMapFilesLocation = temporaryFileNames

				rt.stage = 1
				m.reduceTasks[i] = rt
				// fmt.Printf("Reduce task started running %+v\n", rt)
				go TenSecondsCheckReduce(rt.taskId, m)
				return nil
			}
		}
	} else {
		// if all tasks are done
		// fmt.Println("Map task done....")
		reply.AllTasksDone = true
		return nil
	}
	return nil
}

func (m *Master) checkIfAllMapTasksAreDone() bool {

	for _, mt := range m.mapTasks {
		if mt.stage != 2 {
			// fmt.Println("In check of all map tasks done : false")
			return false
		}
	}
	// fmt.Println("In check of all map tasks done : true")
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

	m.tempMapFilesLocation[args.TaskId] = args.TempMapFilesLocation

	if args.Status == 2 {
		// fmt.Println("Updating the stage of map task of master")
		m.mapTasks[args.TaskId].stage = 2
	} else {
		m.mapTasks[args.TaskId].stage = 0
	}
	// fmt.Printf("After updating the stage, value is %+v\n", m.mapTasks[args.TaskId])
	return nil
}

func (m *Master) ReportOnReduce(args *ReportOnMapToMasterArgs, reply *ReportOnMapToMasterReply) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Status == 2 {
		m.reduceTasks[args.TaskId].stage = 2
	} else {
		m.reduceTasks[args.TaskId].stage = 0
	}

	return nil
}

func TenSecondsCheckMap(taskId int, m *Master) {

	time.Sleep(10 * time.Second)
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.tempMapFilesLocation[taskId]) == m.numberOfReduceTasks {
		m.mapTasks[taskId].stage = 2
	} else {
		m.mapTasks[taskId].stage = 0
	}
	return
}

func TenSecondsCheckReduce(taskId int, m *Master) {

	time.Sleep(10 * time.Second)
	m.mu.Lock()
	defer m.mu.Unlock()

	checkFileName := "mr-out" + strconv.Itoa(taskId)
	if _, err := os.Stat(checkFileName); err == nil {
		m.reduceTasks[taskId].stage = 2
	} else {
		m.reduceTasks[taskId].stage = 0
	}
	return
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.checkIfAllMapTasksAreDone() && m.checkIfAllReduceTasksAreDone() {
		ret = true
	}

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
	// fmt.Println("Number of reducers ", nReduce)
	// initialize map tasks
	m.mapTasks = make([]MapTask, len(files))
	// fmt.Printf("Length of files %d\n", len(files))

	for i, f := range files {
		m.mapTasks[i] = MapTask{taskId: i, stage: 0, fileName: f}
	}
	// fmt.Printf("Maptask stored in master : %+v\n", m.mapTasks[0])

	// initialize reuduce tasks
	m.reduceTasks = make([]ReduceTask, nReduce)

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = ReduceTask{taskId: i, stage: 0}
	}
	m.tempMapFilesLocation = make([][]string, len(files))
	m.server()
	return &m
}
