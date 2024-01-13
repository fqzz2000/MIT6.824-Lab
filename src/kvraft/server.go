package kvraft

import (
	"bytes"
	"log"
	"time"

	// "sync"
	"sync/atomic"

	sync "github.com/sasha-s/go-deadlock"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false
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
	ClerkId int64
	Seq int
}

type identity struct {
	clerkId int64
	seq int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	commitIndex atomic.Int32 // record the index of current committed log index in KVServer, used for restart
	// Your definitions here.
	condMap map[int]chan Err
	db map[string]string
	dupReq map[int64]int
	// dupCommit map[identity]bool
	persister *raft.Persister
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
		op = Op{Opcode: OpPut, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, Seq: args.Seq}
	} else if args.Op == "Append"{
		op = Op{Opcode: OpAppend, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, Seq: args.Seq}
	} else if args.Op == "Get" {
		op = Op{Opcode: OpGet, Key: args.Key, Value: "", ClerkId: args.ClerkId, Seq: args.Seq}
	} else {
		panic("Invalid Op")
	}

	// taking snapshot if needed
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		// DPrintf("[Server %d] Taking Snapshot, current size %v", kv.me, kv.persister.RaftStateSize())
		kv.mu.Lock()
		data := kv.saveSnapshot(int(kv.commitIndex.Load()))
		kv.mu.Unlock()
		kv.rf.Snapshot(int(kv.commitIndex.Load()), data)
		DPrintf("[Server %d, commitIndex %v] Snapshot Taken, current size %v", kv.me, kv.commitIndex.Load(), kv.persister.RaftStateSize())
	}
	// 2. call raft.Start()
	reply.Err = ErrWrongLeader
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("[Server %d] PutAppend(%v) is not leader\n", kv.me, args)
		return
	}
	// check duplicate
	// GET is not duplicate
	kv.mu.Lock()
	DPrintf("[Server %d] PutAppend(%v) called, current db = %v\n", kv.me, args, SPrintMap(kv.db))
	kv.mu.Unlock()
	DPrintf("[Server %d] PutAppend(%v) is leader, index = %d\n", kv.me, args, index)
	// initialize cond
	kv.mu.Lock()
	ch := make(chan Err, 1)
	kv.condMap[index] = ch
	DPrintf("[Server %d] PutAppend(%v) condMap[%d] initialized\n", kv.me, args, index)
	kv.mu.Unlock()
	// 3. wait for raft to commit
	// we should not simply go ahead after the channel be created, it is possible that the given index was commited to the wrong value
	// because the server may no longer the server when the unblock happens
	// moreover it is possible that the server is no longer the leader thus it will never be unblocked
	var res Err
	// If we failed to commit the log entry, simply ask the client to retry another server
	select {
		case <-time.After(time.Millisecond * 100):
			DPrintf("[Server %d] PutAppend(%v) condMap[%d] timeout\n", kv.me, args, index)
			res = ErrWrongLeader
		case res = <-ch:
	} 
	
	// delete cond
	kv.mu.Lock()
	delete(kv.condMap, index)
	close(ch)
	strdb := SPrintMap(kv.db)
	kv.mu.Unlock()
	reply.Err = OK
	if res == ErrWrongLeader {
		reply.Err = ErrWrongLeader
		return 
	}
	if res == ErrDupReq {
		reply.Err = ErrDupReq
		return
	}
	if args.Op == "Get" {
		if val, ok := kv.db[args.Key]; !ok {
			reply.Value = ""
			reply.Err = ErrNoKey
		} else {
			reply.Value = val
			reply.Err = OK
		}
	}

	// 4. return
	DPrintf("[Server %d] PutAppend(%v) done, reply err is (%v) current db = %v\n", kv.me, args, reply.Err, strdb)
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
		if msg.SnapshotValid {
			// load snapshot
			kv.loadSnapshot(msg.Snapshot)
		}
		if msg.CommandValid {
			kv.mu.Lock()
			// wake up the goroutine
			DPrintf("[Server %d] receive msg %v current db %v", kv.me, msg, kv.db)
			// DPrintf("[server %d] dereferencing msg index %d", kv.me, msg.CommandIndex)
			
			if op, ok := msg.Command.(Op); ok {
				if msg.CommandIndex > int(kv.commitIndex.Load()) {
					kv.commitIndex.Store(int32(msg.CommandIndex))
				}
				// update db
				if commitSeq, ok := kv.dupReq[op.ClerkId]; ok && op.Seq <= commitSeq   {
					DPrintf("[Server %d] duplicate commit %d", kv.me, msg.CommandIndex)
					// need to check if there are PRC blocked at the index
					if _, ok := kv.condMap[msg.CommandIndex]; ok && op.Opcode != OpGet {
						kv.condMap[msg.CommandIndex] <- ErrDupReq
						kv.mu.Unlock()
						continue
					}
				} else {
					// update clearkId
					if op.Seq > kv.dupReq[op.ClerkId] {
						kv.dupReq[op.ClerkId] = op.Seq
					}
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
			}
			// DPrintf("[Server %d]Unblocking Command Index %d", kv.me, msg.CommandIndex)
					// check if current server is still leader
			if _, ok := kv.condMap[msg.CommandIndex]; !ok {
				// DPrintf("[server %d] Command Index %d Not Exist", kv.me, msg.CommandIndex)
				kv.mu.Unlock()
				continue
			}
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.condMap[msg.CommandIndex] <- OK
			} else {
				kv.condMap[msg.CommandIndex] <- ErrWrongLeader
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
	DPrintf("[Server %d] StartKVServer called\n", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	kv.condMap = make(map[int]chan Err)
	kv.db = make(map[string]string)
	// kv.dupCommit = make(map[identity]bool)
	kv.dupReq = make(map[int64]int)
	kv.commitIndex.Store(1)
	kv.persister = persister
	kv.loadSnapshot(persister.ReadSnapshot())
	DPrintf("[Server %d commitId %v] StartKVServer called, current db = %v\n", me, kv.commitIndex.Load(), SPrintMap(kv.db))
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.commitLoop()


	return kv
}

func (kv *KVServer) loadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var dupReq map[int64]int
	var commitIndex int
	if d.Decode(&db) != nil || d.Decode(&dupReq) != nil || d.Decode(&commitIndex) != nil {
		panic("Error in decoding snapshot")
	} else {
		kv.db = db
		kv.dupReq = dupReq
		kv.commitIndex.Store(int32(commitIndex))
	}
}

func (kv *KVServer) saveSnapshot(index int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.dupReq)
	e.Encode(kv.commitIndex.Load())
	data := w.Bytes()
	return data
}