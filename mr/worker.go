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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doMap(newTask NewTaskResponse, nReduce int, openedFiles []*os.File, mapf func(string, string) []KeyValue) {

	// Open and read input file
	file, err := os.Open(newTask.MapFileName)
	if err != nil {
		log.Fatalf("cannot open %v\n", newTask.MapFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v\n", newTask.MapFileName)
	}
	file.Close()

	// Apply map operation
	intermediateM := mapf(newTask.MapFileName, string(content))

	// Sort input (for distributed merge in reduce)
	sort.Sort(ByKey(intermediateM))

	// Encode intermediate files in json
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		encoders[i] = json.NewEncoder(openedFiles[i])
	}

	for _, iMap := range intermediateM {
		partitionNumber := ihash(iMap.Key) % nReduce
		err := encoders[partitionNumber].Encode(iMap)
		if err != nil {
			log.Fatalf("Can't save in json file %+v\n", newTask)
		}
	}
}
func doReduce(newTask NewTaskResponse, reducef func(string, []string) string) *os.File {
	var intermediateRLists [][]KeyValue

	// Open reduce files by workers and read it
	for workerIdx := range newTask.ReduceInputIdxs {

		intermediateR := []KeyValue{}

		iname := fmt.Sprintf("mr-%d-%d", workerIdx, newTask.TaskIdx)
		ifile, err := os.Open(iname)
		if err != nil {
			log.Fatalf("Error opening json file %s\n", iname)
		}

		dec := json.NewDecoder(ifile)

		// Assumption: All data is saved within memory, and no external sort is required
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediateR = append(intermediateR, kv)
		}

		intermediateRLists = append(intermediateRLists, intermediateR)

		ifile.Close()
	}

	// TODO: Apply merge sort to intermediateRLists faster (divide-and-conquer to divide and conquer)
	intermediateMergedR := []KeyValue{}

	for _, intermediateRList := range intermediateRLists {
		intermediateMergedR = append(intermediateMergedR, intermediateRList...)
	}

	sort.Sort(ByKey(intermediateMergedR))

	// Save the files, via creating a temporary file first, in case two of these run at once
	ofileName := fmt.Sprintf("temp-out-%d", newTask.TaskIdx)
	ofile, _ := os.CreateTemp("./", ofileName)

	i := 0
	for i < len(intermediateMergedR) {
		j := i + 1
		for j < len(intermediateMergedR) && intermediateMergedR[j].Key == intermediateMergedR[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateMergedR[k].Value)
		}
		output := reducef(intermediateMergedR[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediateMergedR[i].Key, output)

		i = j
	}

	return ofile
}

func startWorker(nReduce int, workerIdx int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var openedFiles []*os.File
	var tempFile *os.File

	// Create/open the files corresponding to a worker on map
	// There are no temp files because renaming and appending would be expensive
	// Although this poses a limit on the maximum file stubs, ideal case assumption taken of < 10000 total files
	for i := 0; i < nReduce; i++ {
		oname := getMRFileName(workerIdx, i)
		var ofile *os.File
		if _, err := os.Stat(oname); err != nil {
			if ofile, err = os.Create(oname); err != nil {
				log.Fatalf("Error creating file for worker %d and partitition %d\n", workerIdx, i)
			}
		} else {
			if ofile, err = os.Open(oname); err != nil {
				log.Fatalf("Error opening file for worker %d and partitition %d\n", workerIdx, i)
			}
		}
		openedFiles = append(openedFiles, ofile)
	}
taskSelection:
	for {

		newTask := AskTask(workerIdx)

		// In case all tasks are completed and there is no connection
		if newTask.CurrentTask == NoProc {
			newTask.CurrentTask = DoneProc
		}

		if newTask.WaitForNewTask {
			continue
		}

		switch newTask.CurrentTask {
		case MapProc:
			doMap(newTask, nReduce, openedFiles, mapf)
			// Flush to disk device after each map
			for i := 0; i < nReduce; i++ {
				if err := openedFiles[i].Sync(); err != nil {
					log.Fatalf("Error in sync %s\n", openedFiles[i].Name())
				}
			}

		case ReduceProc:
			tempFile = doReduce(newTask, reducef)
			defer os.Remove(tempFile.Name())

			finalPath := fmt.Sprintf("mr-out-%d", newTask.TaskIdx)

			// Assumption: os.Rename is an atomic function
			if err := os.Rename(tempFile.Name(), finalPath); err != nil {
				log.Fatalf("ReduceProc: crashed before renaming %s\n", finalPath)
				continue
			}
		case DoneProc:
			// Close all file stubs created during map phase
			for i := 0; i < nReduce; i++ {
				openedFiles[i].Close()
			}
			break taskSelection
		}

		args := TaskStatus{
			TaskProc:  newTask.CurrentTask,
			WorkerIdx: count32(workerIdx),
			TaskIdx:   count32(newTask.TaskIdx),
		}
		CallTaskComplete(args)
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker instance
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Initialize and start new worker
	workerParams := CallInitWorker()

	workerIdx := workerParams.WorkerIdx
	nReduce := workerParams.NReduce
	startWorker(nReduce, workerIdx, mapf, reducef)

}

func CallInitWorker() InitWorker {
	args := struct{}{}
	reply := InitWorker{}
	call("Coordinator.InitWorker", args, &reply)
	return reply
}

func AskTask(workerIdx int) NewTaskResponse {
	args := WorkerIdxArg{WorkerIdx: count32(workerIdx)}
	reply := NewTaskResponse{}
	call("Coordinator.GetTask", args, &reply)
	return reply
}

func CallTaskComplete(args TaskStatus) {
	reply := struct{}{}
	call("Coordinator.CompleteTaskSignal", args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return
	}

	log.Fatalf("%s: call failed with error - %+v\n", rpcname, err)
}
