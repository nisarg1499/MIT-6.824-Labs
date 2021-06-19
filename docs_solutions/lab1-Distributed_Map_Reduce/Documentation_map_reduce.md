# Implementation

There are 3 main files in which the code of the Map-Reduce is implemented. 
In mr folder:

 - rpc.go : All required structures for the RPC Calls
 - worker.go : Implementation of worker
 - master.go : Implementation of master

## Overview
The below image has the complete flow of the application.

![image](https://github.com/nisarg1499/MIT-6.824-Labs/blob/main/docs_solutions/lab1-Distributed_Map_Reduce/system-overview.jpg)


## Details 

### rpc.go
[Code Link](https://github.com/nisarg1499/MIT-6.824-Labs/blob/main/src/mr/rpc.go)

This file includes all RPC definitions. In total 4 definitions were declared and used. 

 1. GetTaskArgs : Arguments passed while requesting task from master 
 2. GetTaskReply : From master to worker in reply to asking of task  
 3. ReportOnMapToMasterArgs : Arguments passed while reporting master on completion of map/reduce job by worker
 4. ReportOnMapToMasterReply : From master to worker in reply of report of map/reduce job 
 
 GetTaskArgs and ReportOnMapToMasterReply are empty structs.
```
type  GetTaskReply  struct {
	TaskId 					int			-> task id from master to worker  
	FileName 				string		-> fetched from MapTask array and sent to worker
	TaskType 				string		-> map/reduce type
	AllTasksDone 			bool		-> used to check the termination of program
	NumberOfReducers 		int			-> used for creating the temp files for storing map (mr-X-Y) where Y is reduce task number
	TempMapFilesLocation 	[]string	-> used for reading files in reduce task
}
```

```
type  ReportOnMapToMasterArgs  struct {
	Status					int			-> 0(not started)/1(running)/2(completed)
	TaskId 					int			-> update the status of particular taskId in master struct, also store temp file location in master struct based on task id (only for map)
	TempMapFilesLocation 	[]string	-> temp file locations generated during map tasks
}
```	

---


### master.go
[Code Link](https://github.com/nisarg1499/MIT-6.824-Labs/blob/main/src/mr/master.go)

This file includes implementation of all methods required by master. 

The important structures included in master are : 
```
type  Master  struct {
	mapTasks 				[]MapTask		-> store all map tasks (MapTask struct)
	reduceTasks 			[]ReduceTask	-> store all reduce tasks (ReduceTask struct)
	mu 						sync.Mutex		-> lock
	numberOfReduceTasks 	int				-> number of reduce tasks
	tempMapFilesLocation 	[][]string		-> all temp file locations (sent after map operation -> for further use in reduce)
}
```
```
type  MapTask  struct {
	taskId 		int		-> unique id for every tasks
	stage 		int		-> status of the task, 0/1/2
	fileName 	string	-> file names stored during initialisation of every task
}
```
```
type  ReduceTask  struct {
	taskId 	int		-> unique id for every tasks
	stage 	int		-> status of the task, 0/1/2
}
```

 #### 1. MakeMaster Function
 
 This function initializes the master (assign values to Master Struct). In this function, it assigns total number of reduce tasks; iterate over the files and initialize the mapTasks by assigning the index of file as taskId, stage as 0 and fileName. Same goes with reduceTasks.
   
 #### 2. GetWorkerTask Function
 - Check if all map/reduce tasks are completed
 - Fetch the map/reduce tasks from mapTasks/reduceTasks from Master and send appropriate parameters in the reply to worker
 - Reply variables on map task : TaskId, FileName, TaskType, AllTasksDone and NumberOfReducers
 - Reply variables on reduce task : TaskId, TaskType, AllTasksDone, NumberOfReducers, TempMapFilesLocation
 - Change the state of the stage in map/reduce tasks
 - Call TenSecondsCheck{Map/Reduce} function as a go-routine to check the status of the task
 - At the end mark AllTasksDone as true 
 
 #### 3. ReportOnMap/ReportOnReduce Function
 - For the given taskId, store the TempMapFileLocation sent in args to the tempMapFilesLocation of master struct. (This step is only for Map Task)
 - Check the status of the task, and update it in the mapTasks  
 
 #### 4. TenSecondsCheckMap/ TenSecondsCheckReduce Function
 - Sleep for 10 seconds
 - For Map : Check the length of array of tempMapFilesLocation of particular taskId with the number of reduce tasks. If same, it means the task with that taskId has been successfully completed.
 - For Reduce : Check whether the file exists with `"mr-out" + (taskId)` name. If it exists then task has been successfully executed. 
 
 #### 5. Done Function
 
 Check if all map tasks and reduce tasks have been completed -> If so, then do `ret = true`.

---


### worker.go
[Code Link](https://github.com/nisarg1499/MIT-6.824-Labs/blob/main/src/mr/worker.go)

This file includes the implementation of all methods required by worker. 

The structures included in this file is KeyValue struct. It is used for storing key values of map.
```
type  KeyValue  struct {
	Key 	string
	Value 	string
}
```
 
#### 1. Worker function
 
Initially, the worker will ask for a task from master, and based on the results and AllTasksDone variable, the loop will be terminated. First, all the map tasks will be assigned, completed and reported back to master. After that all reduce tasks will be assigned, completed and reported to the master. 

#### 2. Map function

Below are the main steps of runMap function : 
- Open the file, read contents and close it
-  Execute map function by passing fileName(fetched from task) and the contents of the file
- Make partitions of X*Y where X is map number task and Y is reduce number task
- Create a tempFile, store the map results, rename it with `mr-x-y`.
- Append the fileName into intermediateLocations variable which will be used while reporting map to master
 
#### 3. Reduce function 

Below are the main steps of runReduce function : 
- Open the file, use Decoder to read the exact json, and append the output in intermediate variable.
- Sort by key
- In a loop, call the reduce function and then store the final output in a file named `mr-out-taskId` 	
