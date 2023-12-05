package mr

import (
	"container/list"
	"fmt"
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

// coordinator stages
const (
	mapStage int32 = iota
	reduceStage
	stopStage
)

type TaskStatus struct {
	Status int
	FilePath []string
}


type WorkerStatus struct {
	lastHeartBeat time.Time
	status int
}


// Coordinator holds all the state that the coordinator needs to keep track of.
// Coordinator maintained three stages: map, reduce, stop
// Map stage: coordinator assign map task to worker, reasign if worker lose connection
// Reduce stage: coordinator assign reduce task to worker, reasign if worker lose connection
// Stop stage: all workers have completed reduce task, then exit
type Coordinator struct {
	// Your definitions here.

	// number of reduce tasks
	nReduce int
	// worker status
	worker_status map[string]WorkerStatus
	// worker status lock
	worker_status_lock sync.Mutex

	// map task status
	map_task_status map[int]TaskStatus
	// map task status lock
	map_task_status_lock sync.Mutex

	// reduce task status
	reduce_task_status map[int]TaskStatus
	// reduce task status lock
	reduce_task_status_lock sync.Mutex

	// idle map task queue
	idle_map_task_queue *list.List
	// idle map task queue lock
	idle_map_task_queue_lock sync.Mutex
	// idle reduce task queue
	idle_reduce_task_queue *list.List
	// idle reduce task queue lock
	idle_reduce_task_queue_lock sync.Mutex
	// test implementation
	test_queue []MrTask
	// atomic variable for Coordinator Stages
	stage atomic.Int32
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
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// // pop a task from test_queue
	// if len(c.test_queue) > 0 {
	// 	reply.Task = c.test_queue[0]
	// 	c.test_queue = c.test_queue[1:]
	// } else {
	// 	reply.Task = MrTask{FilePath: "", MrType: -1}
	// }
	if mapStage == c.stage.Load() {
		// if map stage, pop a task from idle_map_task_queue
		if c.idle_map_task_queue.Len() > 0 {
			// pop a task from idle_map_task_queue
			c.idle_map_task_queue_lock.Lock()
			e := c.idle_map_task_queue.Front()
			reply.Task = e.Value.(MrTask)
	
			fmt.Printf("pop task %v from idle_map_task_queue\n", reply.Task)
			c.idle_map_task_queue.Remove(e)
			c.idle_map_task_queue_lock.Unlock()
			// update map task status
			c.map_task_status_lock.Lock()
			c.map_task_status[reply.Task.MapId] = TaskStatus{Status: pending}
			c.map_task_status_lock.Unlock()
			// update worker status
			c.updateWorkerStat(args.WorkerId, time.Now(), working)
		} else {
			// if idle_map_task_queue is empty, return an wait task
			reply.Task = MrTask{FilePath: "", MrType: Empty_t}
		}
	} else if reduceStage == c.stage.Load() {
		// TOBE IMPLEMENTED
	} else if stopStage == c.stage.Load() {
		// if stop stage, return an stop task
		reply.Task = MrTask{FilePath: "", MrType: Stop_t}
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
// a tcp server to print worker status
func (c *Coordinator) monitorServer() {
	l, e := net.Listen("tcp", ":8580")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("accept error:", err)
		}
		fmt.Printf("worker status: %v\n", c.worker_status)
		
		conn.Close()
	}
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
	c := Coordinator{nReduce: nReduce}

	// Your code here.
	// Initialize coordinator

	c.stage.Store(mapStage)
	c.worker_status = make(map[string]WorkerStatus)
	c.map_task_status = make(map[int]TaskStatus)
	c.reduce_task_status = make(map[int]TaskStatus)
	// initialize idle map task queue
	c.idle_map_task_queue = list.New()
	// initialize idle reduce task queue
	c.idle_reduce_task_queue = list.New()
	// initialize map task queue
	for i, file := range files {
		c.idle_map_task_queue.PushBack(MrTask{FilePath: file, MrType: Map_t, MapId: i, ReduceId: i, NReduce: nReduce})
	}



	// // temp implementation for testing
	// c.test_queue = make([]MrTask, 3)
	// c.test_queue[0] = MrTask{FilePath: "../test.txt", MrType: Map_t, MapId: 0, ReduceId: 0}
	// c.test_queue[1] = MrTask{FilePath: "../test.txt", MrType: Reduce_t, MapId: 0, ReduceId: 0}
	// c.test_queue[2] = MrTask{FilePath: "../test.txt", MrType: Stop_t, MapId: 0, ReduceId: 0}
	c.server()
	go c.healthCheck()
	return &c
}

// RPC handler for worker to send heartbeat to coordinator
func (c *Coordinator) WorkerHeartBeat(args *HeartBeatArgs, reply *EmptyArgs) error {
	// update worker status
	c.updateWorkerStat(args.WorkerId, time.Now(), args.Status)
	return nil
}

func (c *Coordinator) updateWorkerStat(WorkerId string, timestamp time.Time, status int) {
	c.worker_status_lock.Lock()
	c.worker_status[WorkerId] = WorkerStatus{lastHeartBeat: timestamp, status: status}
	c.worker_status_lock.Unlock()
	fmt.Printf("worker status of %v updated to %v\n", WorkerId, status)
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
		
		if stopStage == c.stage.Load() {
			break
		}
	}
}

// RPC handler for worker to send map complete message to coordinator
func (c *Coordinator) ReportMapComplete(args *TaskCompleteArgs, reply *EmptyArgs) error {
	c.map_task_status_lock.Lock()
	c.map_task_status[args.Id] = TaskStatus{Status: complete, FilePath: args.IFilePath}
	c.map_task_status_lock.Unlock()
	// update worker status
	c.updateWorkerStat(args.WorkerId, time.Now(), idle)
	return nil
}

// RPC handler for worker to send reduce complete message to coordinator
func (c *Coordinator) ReportReduceComplete(args *TaskCompleteArgs, reply *EmptyArgs) error {
	c.reduce_task_status_lock.Lock()
	c.reduce_task_status[args.Id] = TaskStatus{Status: complete, FilePath: args.IFilePath}
	c.reduce_task_status_lock.Unlock()
	// update worker status
	c.updateWorkerStat(args.WorkerId, time.Now(), idle)
	return nil
}

