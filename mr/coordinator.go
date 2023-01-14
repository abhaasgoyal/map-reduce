package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

/* https://stackoverflow.com/a/22035083 */
type count32 int32

func (c *count32) inc() int32 {
	return atomic.AddInt32((*int32)(c), 1)
}

func (c *count32) get() int32 {
	return atomic.LoadInt32((*int32)(c))
}

func (c *count32) set(val int32) {
	atomic.StoreInt32((*int32)(c), val)
}

type Coordinator struct {

	/* Input parameters */
	inputFileNames []string
	nReduce        int // Total number of reduce tasks

	currentTask    TaskProc
	currentTaskMut sync.Mutex

	/* New Workers get assigned a unique idx */
	workerCounter     count32 // Total number of different workers assigned until now
	mapFileCounter    int     // Current new input file for map task
	reduceFileCounter int     // Current new partition fileset for reduce task
	counterMut        sync.Mutex

	/* Track number of completed tasks in the current task type */
	tasksCompleted count32

	/* Channels */
	// Notify Coordinator that a task has been successfully completed
	// Note: The channel will only be used once for each TaskIdx
	successCh chan TaskStatus
	// When worker asks for tasks, the coordinator checks for any pending tasks
	pendingCh chan TaskStatus
	// Bookkeeper for completed tasks that were done on time
	statusMarkComplete []chan TaskStatus
	// A signal to indicate that the full job has been completed
	completeJob chan bool

	/* Barrier synchronizations after map and reduce */
	mapJobWG    sync.WaitGroup // Barrier synchronization after map
	reduceJobWG sync.WaitGroup // Barrier synchronization after reduce

	// Set of successful reducer files for each reduce Index
	intermediateReducerIdxs     map[int]struct{}
	intermediateReducerFilesMut sync.RWMutex

	// Reinitialize states when task types go from map -> reduce
	resetOnce sync.Once
}

func taskCheck(c *Coordinator) {

selection:
	for {
		select {
		case success := <-c.successCh:

			if success.TaskProc == MapProc {
				c.mapJobWG.Done()
			} else if success.TaskProc == ReduceProc {
				c.reduceJobWG.Done()
			}

			c.tasksCompleted.inc()

			if success.TaskProc == MapProc {
				// Setup a successful set of workerIdx partition files for reduce tasks
				c.intermediateReducerFilesMut.Lock()
				c.intermediateReducerIdxs[int(success.WorkerIdx)] = struct{}{}
				c.intermediateReducerFilesMut.Unlock()
			}
		case <-c.completeJob: // When all jobs are completed
			break selection
		}
	}

}

func newTaskStatusChecker(c *Coordinator, taskIdx int, workerIdx count32, taskProc TaskProc) {
	select {
	case success := <-c.statusMarkComplete[taskIdx]:
		// Send to successful task checker
		c.successCh <- success
	case <-time.After(10 * time.Second):
		// Assume that the worker has died
		pendingStatus := TaskStatus{TaskIdx: count32(taskIdx), WorkerIdx: workerIdx, TaskProc: taskProc}
		c.pendingCh <- pendingStatus
	}
}

// RPC handlers for the worker to call.
func (c *Coordinator) InitWorker(args struct{}, reply *InitWorker) error {

	currCount := c.workerCounter.inc()

	// Craft reply
	reply.WorkerIdx = int(currCount)
	reply.NReduce = c.nReduce

	return nil
}

func (c *Coordinator) GetTask(args WorkerIdxArg, reply *NewTaskResponse) error {

	var currFilePtr *int
	var currFileIdx int
	var totalFileGroups int

	c.currentTaskMut.Lock()
	reply.CurrentTask = c.currentTask
	c.currentTaskMut.Unlock()

	if reply.CurrentTask == DoneProc {
		return nil
	}

	if reply.CurrentTask == MapProc {
		totalFileGroups = len(c.inputFileNames)
		currFilePtr = &c.mapFileCounter
	} else {
		totalFileGroups = c.nReduce
		currFilePtr = &c.reduceFileCounter
	}

selection:
	for {
		select {
		// Check for the initial case of previously failed jobs
		case failedTask := <-c.pendingCh:
			reply.CurrentTask = failedTask.TaskProc
			currFileIdx = int(failedTask.TaskIdx)
			break selection
		default:
			// Normally wait for newly handed out indexes
			if c.tasksCompleted.get() >= int32(totalFileGroups) {
				if reply.CurrentTask == MapProc {
					ResetTaskCounter(c)
				} else {
					c.currentTaskMut.Lock()
					c.currentTask = DoneProc
					c.currentTaskMut.Unlock()
				}
				reply.WaitForNewTask = true
				return nil
			}
			c.counterMut.Lock()
			currFileIdx = *currFilePtr
			if *currFilePtr < totalFileGroups {
				*currFilePtr++
			}
			c.counterMut.Unlock()
			if currFileIdx < totalFileGroups {
				break selection
			}
		}
	}

	// Craft remaining TaskParams
	if reply.CurrentTask == MapProc {
		reply.MapFileName = c.inputFileNames[currFileIdx]
	} else if reply.CurrentTask == ReduceProc {
		c.intermediateReducerFilesMut.RLock()
		reply.ReduceInputIdxs = c.intermediateReducerIdxs
		c.intermediateReducerFilesMut.RUnlock()
	}

	reply.TaskIdx = currFileIdx

	go newTaskStatusChecker(c, currFileIdx, args.WorkerIdx, reply.CurrentTask)

	return nil
}

func ResetTaskCounter(c *Coordinator) {
	c.mapJobWG.Wait()

	reduceReset := func() {
		c.currentTaskMut.Lock()
		c.currentTask = ReduceProc
		c.tasksCompleted.set(0)
		c.currentTaskMut.Unlock()
	}

	c.resetOnce.Do(reduceReset)
}

func (c *Coordinator) CompleteTaskSignal(args TaskStatus, reply *struct{}) error {
	// Book-keeping for ensuring that the task was done on time
	c.statusMarkComplete[args.TaskIdx] <- args
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		log.Fatalf("Error in registering %+v in rpc\n", c)
	}
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go serve(&l)
}

func serve(l *net.Listener) {
	if err := http.Serve(*l, nil); err != nil {
		log.Fatal("http server error:", err)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.reduceJobWG.Wait()
	c.completeJob <- true
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	statusMarkCompleteLen := nReduce
	if len(files) > nReduce {
		statusMarkCompleteLen = len(files)
	}

	c := Coordinator{
		inputFileNames:          files,
		nReduce:                 nReduce,
		currentTask:             MapProc,
		workerCounter:           -1,
		mapFileCounter:          0,
		reduceFileCounter:       0,
		tasksCompleted:          0,
		successCh:               make(chan TaskStatus),
		pendingCh:               make(chan TaskStatus),
		completeJob:             make(chan bool),
		statusMarkComplete:      make([]chan TaskStatus, statusMarkCompleteLen),
		intermediateReducerIdxs: make(map[int]struct{}),
	}

	for i := range c.statusMarkComplete {
		c.statusMarkComplete[i] = make(chan TaskStatus)
	}

	// Barrier synchronization for map and reduce
	c.mapJobWG.Add(len(files))
	c.reduceJobWG.Add(nReduce)

	go taskCheck(&c)
	// Initialize server
	c.server()
	return &c
}
