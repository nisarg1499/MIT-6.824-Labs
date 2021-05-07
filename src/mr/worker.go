package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	masterStatus := call("Master.GetWorkerTask", &args, &reply)

	for masterStatus == true && !reply.AllTasksDone {
		if reply.TaskType == "Map" {
			// fmt.Printf("Map running on task %d\n", reply.TaskId)

			// execute map function
			intermediateLocations, err := runMap(mapf, &reply)

			mapReportArgs := ReportOnMapToMasterArgs{}
			mapReportReply := ReportOnMapToMasterReply{}
			// in reponse of map function check whether it was success or not, if not then change status to 0

			// fmt.Println("Err variable from map : ", err)
			if err == nil {
				// fmt.Println("In status = 2")
				mapReportArgs.Status = 2
			} else {
				mapReportArgs.Status = 0
			}
			mapReportArgs.TaskId = reply.TaskId
			mapReportArgs.TempMapFilesLocation = intermediateLocations

			// after success report back to master
			call("Master.ReportOnMap", &mapReportArgs, &mapReportReply)
		} else if reply.TaskType == "Reduce" {
			// fmt.Printf("Reduce running on task %v\n", reply.TaskId)

			// execute reduce function
			err := runReduce(reducef, &reply)

			mapReportArgs := ReportOnMapToMasterArgs{}
			mapReportReply := ReportOnMapToMasterReply{}

			// in reply check status of reduce, if failed then update master to run it again
			if err == nil {
				mapReportArgs.Status = 2
			} else {
				mapReportArgs.Status = 0
			}
			mapReportArgs.TaskId = reply.TaskId

			// after success report back to master
			call("Master.ReportOnReduce", &mapReportArgs, &mapReportReply)
		} else {
			// fmt.Printf("No task received from master..\n")
		}

		args = GetTaskArgs{}
		reply = GetTaskReply{}

		masterStatus = call("Master.GetWorkerTask", &args, &reply)
	}

}

func runMap(mapf func(string, string) []KeyValue, task *GetTaskReply) ([]string, error) {

	inputFileName := task.FileName
	// fmt.Printf("File Name : %v\n", inputFileName)
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
	// fmt.Printf("My kva from map : %+v\n", kva)

	// We need to make NumberOfReducers files to store maps
	data := make([][]KeyValue, task.NumberOfReducers)

	// We need to make a partition of X*Y where X is Map task number and Y is reduce task number.
	for x := 0; x < len(kva); x++ {
		y := ihash(kva[x].Key) % int(task.NumberOfReducers) // To make sure number is in the range
		data[y] = append(data[y], kva[x])
	}

	var intermediateLocations []string
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
		os.Rename(tempFile.Name(), "mr-"+fmt.Sprint(task.TaskId)+"-"+fmt.Sprint(y))
		tempFile.Close()
		intermediateLocations = append(intermediateLocations, "mr-"+fmt.Sprint(task.TaskId)+"-"+fmt.Sprint(y))
	}
	// fmt.Println("Map success done")
	return intermediateLocations, nil

}

func runReduce(reducef func(string, []string) string, task *GetTaskReply) error {

	intermediate := []KeyValue{}
	for _, fileName := range task.TempMapFilesLocation {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// Just for debug purpose
	// for a := 0; a < len(intermediate); a++ {
	// 	fmt.Printf("Key Value after decoding %+v", intermediate[a])
	// }

	// took mrsequential reduce code
	sort.Sort(ByKey(intermediate))

	ofile, err := ioutil.TempFile("", "mr-out")
	oname := ofile.Name()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	finalName := "mr-out-" + fmt.Sprint(task.TaskId)
	// atomically rename temporary(out named) file
	err = os.Rename(oname, finalName)
	if err != nil {
		// fmt.Printf("Failed to rename map file %v to %v", oname, finalName)
		return err
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
