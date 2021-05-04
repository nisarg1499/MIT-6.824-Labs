package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id int
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	call("Master.GetWorkerTask", &args, &reply)

	for !reply.AllTasksDone {
		if reply.TaskType == "Map" {
			fmt.Printf("Map running on task %v\n", reply.TaskId)

			// execute map function
			// runMap()

			// in reply of map function check whether it was success or not, if not then change status to running and report master to run it again

			// after success report back to master
		} else if reply.TaskType == "Reduce" {
			fmt.Printf("Reduce running on task %v\n", reply.TaskId)

			// execute reduce function
			// runReduce()

			// in reply check status of reduce, if failed then update master to run it again

			// after success report back to master
		} else {
			fmt.Printf("No task received from master..\n")
		}

		args = GetTaskArgs{}
		reply = GetTaskReply{}

		call("Master.GetWorkerTask", &args, &reply)
	}

}

func runMap(mapf func(string, string) []KeyValue, task *GetTaskReply) error {

	inputFileName := task.FileName

	// reading files and stuff taken from mrsequential file
	file, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("cannot open %v", inputFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFileName)
	}
	file.Close()
	// Execute map function and store key values
	kva := mapf(inputFileName, string(content))

	// We need to make NumberOfReducers files to store maps
	data := make([][]KeyValue, task.NumberOfReducers)

	// We need to make a partition of X*Y where X is Map task number and Y is reduce task number.
	for x := 0; x < len(kva); x++ {
		y := ihash(kva[x].Key) % int(task.NumberOfReducers) // To make sure number is in the range
		data[y] = append(data[y], kva[x])
	}

	// Now we have distributed files based on keys of map, now we need to save these files as temporary files
	for y := 0; y < len(data); y++ {
		tempFile, _ := ioutil.TempFile(".", "")
		enc := json.NewEncoder(tempFile)
		for _, keyValuePair := range data[y] {
			err := enc.Encode(&keyValuePair)
			if err != nil {
				log.Fatalf("cannot encode json %v", keyValuePair.Key)
			}
		}
		os.Rename(tempFile.Name(), "mr-"+fmt.Sprint(task.Index)+"-"+fmt.Sprint(y))
		ofile.Close()
	}
	return nil

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
