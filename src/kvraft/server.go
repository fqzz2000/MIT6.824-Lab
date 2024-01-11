package kvraft

import (
	"log"
	// "sync"
	"sync/atomic"

	sync "github.com/sasha-s/go-deadlock"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opcode int
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	condMap map[int]chan bool
	db map[string]string

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	
	defer kv.mu.Unlock()
	// DPrintf("[Server %d] Get(%v) db = %v\n", kv.me, args, SPrintMap(kv.db))
	if val, ok := kv.db[args.Key]; ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}
	return 
}

func SPrintMap(m map[string]string) string {
	if m == nil {
		return "nil"
	}
	s := "{"
	for k, v := range m {
		s += "(" + k + ":" + v + ")"
	}
	s += "}"
	return s
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 1. create op
	var op Op
	if args.Op == "Put" {
		op = Op{Opcode: OpPut, Key: args.Key, Value: args.Value}
	} else if args.Op == "Append"{
		op = Op{Opcode: OpAppend, Key: args.Key, Value: args.Value}
	}
	// 2. call raft.Start()
	reply.Err = ErrWrongLeader
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	// DPrintf("[Server %d] PutAppend(%v) is leader, index = %d\n", kv.me, args, index)
	// initialize cond
	kv.mu.Lock()
	ch := make(chan bool)
	kv.condMap[index] = ch
	// DPrintf("[Server %d] PutAppend(%v) condMap[%d] initialized\n", kv.me, args, index)
	kv.mu.Unlock()
	// 3. wait for raft to commit
	<-ch
	// DPrintf("[Server %d] PutAppend(%v) condMap[%d] unblocked\n", kv.me, args, index)
	close(ch)
	// delete cond
	kv.mu.Lock()
	delete(kv.condMap, index)
	kv.mu.Unlock()

	reply.Err = OK
	// 4. return
	DPrintf("[Server %d] PutAppend(%v) done, current db = %v\n", kv.me, args, SPrintMap(kv.db))
	return 
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// read committed value from applyCh and apply change to db
func (kv *KVServer) commitLoop() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return 
		}
		if msg.CommandValid {
			kv.mu.Lock()
			// wake up the goroutine
			// DPrintf("[server %d] dereferencing msg index %d", kv.me, msg.CommandIndex)
			if _, ok := kv.condMap[msg.CommandIndex]; !ok {
				// DPrintf("[server %d] Command Index %d Not Exist", kv.me, msg.CommandIndex)
				kv.mu.Unlock()
				continue
			}
			// DPrintf("[Server %d]Unblocking Command Index %d", kv.me, msg.CommandIndex)
			kv.condMap[msg.CommandIndex] <- true
			// update db
			if op, ok := msg.Command.(Op); ok {
				if op.Opcode == OpAppend {
					oldVal, ok := kv.db[op.Key]
					if !ok {
						kv.db[op.Key] = op.Value
					} else {
						kv.db[op.Key] = oldVal + op.Value
					}
				} else if op.Opcode == OpPut {
					kv.db[op.Key] = op.Value	
				}
			}
			kv.mu.Unlock()
		}

	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.condMap = make(map[int]chan bool)
	kv.db = make(map[string]string)

	// You may need initialization code here.
	go kv.commitLoop()

	return kv
}
