package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	killed atomic.Bool

	configs []Config // indexed by config num
	condMap map[int]chan Err
	dupReq map[int64]int
}


type Op struct {
	// Your data here.
	OpCode string
	Args interface{}
	ClerkId int64
	Seq int
}

type OpRes struct {
	Err Err
	Config Config
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

func (sc *ShardCtrler) Operate(args *GlobalArgs, reply *GlobalReply) {
	// Your code here.
	op := Op{OpCode: args.OpCode, Args: args.Args, ClerkId: args.ClerkId, Seq: args.Seq}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	index, _, _ := sc.rf.Start(op)
	ch := make(chan Err, 1)
	sc.condMap[index] = ch
	sc.mu.Unlock()
	var err Err
	select {
	case err = <- ch:
	case <- time.After(200 * time.Millisecond):
		err = ErrTimeout
	}
	sc.mu.Lock()
	close(sc.condMap[index])
	delete(sc.condMap, index)
	sc.mu.Unlock()
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = false
	}
	if args.OpCode == OprQuery {
		OpArgs := args.Args.(QueryArgs)
		if OpArgs.Num == -1 || OpArgs.Num >= len(sc.configs) {
			reply.config = sc.configs[len(sc.configs) - 1]
		} else {
			reply.config = sc.configs[OpArgs.Num]
		}
	}
	return 
}

func (sc *ShardCtrler) applyLoop() {
	for msg := range sc.applyCh {
		if sc.killed.Load() {
			return
		}
		if msg.CommandValid {
			// dealing with duplicate request
			sc.mu.Lock()
			op := msg.Command.(Op)
			if sc.dupReq[op.ClerkId] >= op.Seq {
				if ch, ok := sc.condMap[msg.CommandIndex]; ok && op.OpCode != OprQuery {
					ch <- ErrDupReq
					continue
				}
			} else {
				sc.dupReq[op.ClerkId] = op.Seq
			}
			// apply request
			sc.apply(op)

			// unblocking the RPC 
			// first check if the RPC is still waiting
			if ch, ok := sc.condMap[msg.CommandIndex]; ok {
				// check if the server is still leader
				// consider the case that 
				// 1. client send a request to leader
				// 2. leader offline
				// 3. leader online but new leader is elected
				// in this situation, the RPC should be unblocked with errorWrongLeader
				if _, isLeader := sc.rf.GetState(); isLeader {
					ch <- OK
				} else {
					ch <- ErrWrongLeader
				}
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) apply(op Op) {
	switch op.OpCode {
	case OprJoin:
		joinargs := op.Args.(JoinArgs)
		// create a new config
		newConfig := sc.configs[len(sc.configs) - 1].Copy()
		// add new group
		for gid, servers := range joinargs.Servers {
			if err := newConfig.AddGroup(gid, servers); err != OK {
				panic("Add Group Failed: " + err)
			}
		}
		// rebalance
		newConfig.Rebalance()
	case OprLeave:
		leaveargs := op.Args.(LeaveArgs)
		// create a new config
		newConfig := sc.configs[len(sc.configs) - 1].Copy()
		// remove group
		for _, gid := range leaveargs.GIDs {
			if err := newConfig.RemoveGroup(gid); err != OK {
				panic("Remove Group Failed: " + err)
			}
		}
		// rebalance
		newConfig.Rebalance()
	case OprMove:
		moveargs := op.Args.(MoveArgs)
		// create a new config
		newConfig := sc.configs[len(sc.configs) - 1].Copy()
		// move shard
		if err := newConfig.MoveShard(moveargs.Shard, moveargs.GID); err != OK {
			panic("Move Shard Failed: " + err)
		}
	}
}




// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.killed.Store(true)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
