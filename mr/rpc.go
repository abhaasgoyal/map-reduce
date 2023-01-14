package mr

//
// RPC definitions.
//

import "os"
import "strconv"
import "fmt"

type TaskProc int

const (
	NoProc     TaskProc = 0
	MapProc    TaskProc = 1
	ReduceProc TaskProc = 2
	DoneProc   TaskProc = 3
)

// Initialize a new worker
type InitWorker struct {
	WorkerIdx int // Unique worker id

	// Number of reduce files
	// Used in determining number of partitions during Map stage
	NReduce int //
}

// Ask Coordinator for available tasks for a particular type
type WorkerIdxArg struct {
	WorkerIdx count32 // REVIEW: Maybe move this in response
}

// Coordinator response
type NewTaskResponse struct {
	CurrentTask     TaskProc
	WaitForNewTask  bool
	TaskIdx         int
	MapFileName     string           // Retrieved only during MapProc
	ReduceInputIdxs map[int]struct{} // Set of indexes to use in reduce
}

type TaskStatus struct {
	TaskProc  TaskProc
	TaskIdx   count32
	WorkerIdx count32
}

type FileName = string

func getMRFileName(taskIdx int, reducerIdx int) string {
	return fmt.Sprintf("mr-%d-%d", taskIdx, reducerIdx)
}

// Cook up a unique-ish UNIX-domain socket name
// in mr-tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/mr-sock-"
	s += strconv.Itoa(os.Getuid())
	return s
}
