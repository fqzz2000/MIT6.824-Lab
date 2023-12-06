package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

type workerInstance struct {
	workerId string
	status atomic.Int32
}

func getFileContent(filename string) []byte {
	file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
	return content
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// get task from coordinator
	instance := workerInstance{workerId: ""}
	instance.workerId = Register()
	instance.status.Store(int32(idle))

	go instance.StartHeartBeat()

	LOOP:
	for {
		task := instance.getTask()
		switch task.MrType {
			case Map_t:
				// do map
				instance.status.Store(int32(working))
				content := getFileContent(task.FilePath)
				// call mapf
				kva := mapf(task.FilePath, string(content))
				// write kva to intermediate file
				
				fnames := storeMapResult(kva, task.MapId, task.NReduce)
				// send map complete message to coordinator
				instance.status.Store(int32(idle))
				ReportMapComplete(instance.workerId, task.MapId, fnames)
				

			
			case Reduce_t:
				// do reduce
				instance.status.Store(int32(working))
				// call reducef
				kva := readMapResult(task)
				// sort kva by key
				sort.Sort(ByKey(kva))
				// call reducef
				fnames := storeReduceResult(reducef, kva, task)
				
				// send reduce complete message to coordinator
				instance.status.Store(int32(idle))
				ReportReduceComplete(instance.workerId, task.ReduceId, fnames)
				
				
			case Stop_t:
			// do stop
				break LOOP
			case Empty_t:
				// wait for task
				time.Sleep(1 * time.Second)

			default:
				log.Fatal("unknown task type")
			// do nothing
			}
		}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// RPC handler for worker to send map complete message to coordinator
func  ReportMapComplete(workerId string, mapid int, intermediaFilePath []string) error {
	
	args := TaskCompleteArgs{WorkerId: workerId, Id: mapid, IFilePath: intermediaFilePath}
	ok := call("Coordinator.ReportMapComplete", &args, &EmptyArgs{})
	if !ok {
		fmt.Println("send map complete message failed")
	}
	return nil
}

// RPC handler for worker to send reduce complete message to coordinator
func ReportReduceComplete(workerId string, reduceid int, intermediaFilePath []string) error {
	args := TaskCompleteArgs{WorkerId: workerId, Id: reduceid, IFilePath: intermediaFilePath}
	ok := call("Coordinator.ReportReduceComplete", &args, &EmptyArgs{})
	if !ok {
		fmt.Println("send reduce complete message failed")
	}
	return nil
}

// return intermediate file names
func storeMapResult(kva []KeyValue, mapId int, nReduce int) []string{
	// create intermediate file
	rets := make([]string, 0)
	encs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", mapId, i)
		rets = append(rets, oname)
		ofile, _ := os.Create(oname)
		encs[i] = json.NewEncoder(ofile)
	}
	for _, kv := range kva {
		enc := encs[ihash(kv.Key) % nReduce]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
	return rets
}

func readMapResult(task MrTask) []KeyValue {
	kva := []KeyValue{}
	// read intermediate file
	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	pattern := regexp.MustCompile(`mr-\d+-` + strconv.Itoa(task.ReduceId))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if pattern.MatchString(file.Name()) {
			fva := decodeFile(file.Name())
			kva = append(kva, fva...)
		}
	}
	return kva
}

func storeReduceResult(reducef func(string, []string) string , kva []KeyValue, task MrTask) []string {
	oname := fmt.Sprintf("mr-out-%d", task.ReduceId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	return []string{oname}
}


func decodeFile(iname string) []KeyValue {
	kva := []KeyValue{}
	ifile, _ := os.Open(iname)
	dec := json.NewDecoder(ifile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		kva = append(kva, kv)
	}
	return kva
}

func (w *workerInstance) StartHeartBeat() {
	// send heartbeat every 3 second
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {
		SendHeartBeat(w.workerId, int(w.status.Load()))
	}
}

// RPC handler for worker to send heartbeat to coordinator
func SendHeartBeat(workerId string, status int) {
	args := HeartBeatArgs{WorkerId: workerId, Status: status}
	reply := EmptyArgs{}
	ok := call("Coordinator.WorkerHeartBeat", &args, &reply)
	if !ok {
		fmt.Println("send heartbeat failed")
	}
}

// RPC handler for worker to register to coordinator
func Register() string {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		return reply.WorkerId
	} else {
		return ""
	}
}

// RPC handler for worker to get task from coordinator
func (w *workerInstance) getTask() MrTask {
	args := TaskArgs{WorkerId: w.workerId}
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply.Task
	} else {
		return MrTask{FilePath: "", MrType: -1}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
