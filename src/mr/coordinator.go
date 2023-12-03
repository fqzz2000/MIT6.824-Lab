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

	"github.com/google/uuid"
)

// worker status
const (
	idle int = iota
	working
	loseConnection
)

// task status
const (
	unstarted int = iota
	pending
	complete
)

type MapTaskStatus struct {
	status int
	intermediateFilePath string
}

type WorkerStatus struct {
	lastHeartBeat time.Time
	status int
}
type Coordinator struct {
	// Your definitions here.


	// worker status
	worker_status map[string]WorkerStatus
	// worker status lock
	worker_status_lock sync.Mutex

	// map task status
	map_task_status map[int]MapTaskStatus
	// map task status lock
	map_task_status_lock sync.Mutex
	// idle map task queue
	// test implementation
	test_queue []MrTask
	// atimic variable for server done
	done atomic.Bool

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RPC handler for worker to get task from coordinator
func (c *Coordinator) GetTask(args *ExampleArgs, reply *TaskReply) error {
	// pop a task from test_queue
	if len(c.test_queue) > 0 {
		reply.Task = c.test_queue[0]
		c.test_queue = c.test_queue[1:]
	} else {
		reply.Task = MrTask{FilePath: "", MrType: -1}
	}

	return nil

}

func (c * Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	// register worker
	reply.WorkerId = uuid.New().String()
	c.worker_status_lock.Lock()
	c.worker_status[reply.WorkerId] = WorkerStatus{lastHeartBeat: time.Now(), status: idle}
	c.worker_status_lock.Unlock()
	
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.test_queue = make([]MrTask, 3)
	c.test_queue[0] = MrTask{FilePath: "../test.txt", MrType: Map_t, MapId: 0, ReduceId: 0}
	c.test_queue[1] = MrTask{FilePath: "../test.txt", MrType: Reduce_t, MapId: 0, ReduceId: 0}
	c.test_queue[2] = MrTask{FilePath: "../test.txt", MrType: Stop_t, MapId: 0, ReduceId: 0}
	c.server()
	go c.healthCheck()
	return &c
}

func (c *Coordinator) WorkerHeartBeat(args *HeartBeatArgs, reply *EmptyArgs) error {
	// update worker status
	c.recordHeartBeat(args.WorkerId, time.Now(), args.Status)
	return nil
}

func (c *Coordinator) recordHeartBeat(WorkerId string, timestamp time.Time, status int) {
	c.worker_status_lock.Lock()
	c.worker_status[WorkerId] = WorkerStatus{lastHeartBeat: timestamp, status: status}
	c.worker_status_lock.Unlock()
}

func (c *Coordinator) healthCheck() {
	// every 5 seconds, check if worker is still alive
	// if not, set worker status to loseConnection
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {	
		c.worker_status_lock.Lock()	
		for workerId, workerStatus := range c.worker_status {
			// if haven't received heartbeat for 5 seconds, set status to loseConnection
			if time.Now().Sub(workerStatus.lastHeartBeat) > 5 * time.Second {
				// lose connection
				c.worker_status[workerId] = WorkerStatus{lastHeartBeat: workerStatus.lastHeartBeat, status: loseConnection}
			}
			// if haven't received heartbeat for 10 seconds, delete worker
			if time.Now().Sub(workerStatus.lastHeartBeat) > 10 * time.Second {
				delete(c.worker_status, workerId)
			}
		}
		c.worker_status_lock.Unlock()
		
		if hasDone := c.done.Load(); hasDone {
			break
		}
	}
}

// RPC handler for worker to send map complete message to coordinator
func (c *Coordinator) ReportMapComplete(args *MapCompleteArgs, reply *EmptyArgs) error {
	c.map_task_status_lock.Lock()
	c.map_task_status[args.MapId] = MapTaskStatus{status: complete, intermediateFilePath: args.IFilePath}
	c.map_task_status_lock.Unlock()
	
	// update worker status
	c.recordHeartBeat(args.WorkerId, time.Now(), idle)
	return nil
}

// RPC handler for worker to send reduce complete message to coordinator
func (c *Coordinator) ReportReduceComplete(args *ReduceCompleteArgs, reply *EmptyArgs) error {
	// to be implemented
	return nil
}

