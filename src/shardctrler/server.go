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
	QArgs QueryArgs
	JArgs JoinArgs
	LArgs LeaveArgs
	MArgs MoveArgs
	ClerkId int64
	Seq int
}

type OpRes struct {
	Err Err
	Config Config
}

func (sc *ShardCtrler) Operate(args *GlobalArgs, reply *GlobalReply) {
	// Your code here.
	DPrintf("[Server %d] receive request %v, current Configs %v", sc.me, args, sc.configs)
	op := makeOp(args)
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	if args.OpCode == OprQuery {
		OpArgs := args.QArgs
		if OpArgs.Num != -1 && OpArgs.Num < len(sc.configs) {
			reply.Config = sc.configs[OpArgs.Num]
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
	}
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
		OpArgs := args.QArgs
		if OpArgs.Num == -1 || OpArgs.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs) - 1]
		} else {
			reply.Config = sc.configs[OpArgs.Num]
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

func makeOp(args *GlobalArgs) Op {
	switch args.OpCode {
	case OprJoin:
		return Op{OpCode: args.OpCode, JArgs: args.JArgs, ClerkId: args.ClerkId, Seq: args.Seq}
	case OprLeave:
		return Op{OpCode: args.OpCode, LArgs: args.LArgs, ClerkId: args.ClerkId, Seq: args.Seq}
	case OprMove:
		return Op{OpCode: args.OpCode, MArgs: args.MArgs, ClerkId: args.ClerkId, Seq: args.Seq}
	case OprQuery:
		return Op{OpCode: args.OpCode, QArgs: args.QArgs, ClerkId: args.ClerkId, Seq: args.Seq}
	}
	panic("Invalid Op")
}

func (sc *ShardCtrler) apply(op Op) {
	DPrintf("[Server %d] receive msg %v current config %v", sc.me, op, sc.configs)
	switch op.OpCode {
	case OprJoin:
		joinargs := op.JArgs
		// create a new config
		var newConfig Config
		if len(sc.configs) == 0 {
			newConfig = Config{}
		} else {
			newConfig = sc.configs[len(sc.configs) - 1].Copy()
		}
		newConfig.Num = len(sc.configs)
		// add new group
		for gid, servers := range joinargs.Servers {
			if err := newConfig.AddGroup(gid, servers); err != OK {
				panic("Add Group Failed: " + err)
			}
		}
		// rebalance
		newConfig.Rebalance()
		// append new config
		sc.configs = append(sc.configs, newConfig)
	case OprLeave:
		leaveargs := op.LArgs
		// create a new config
		var newConfig Config
		if len(sc.configs) == 0 {
			newConfig = Config{}
		} else {
			newConfig = sc.configs[len(sc.configs) - 1].Copy()
		}
		newConfig.Num = len(sc.configs)
		// remove group
		for _, gid := range leaveargs.GIDs {
			if err := newConfig.RemoveGroup(gid); err != OK {
				panic("Remove Group Failed: " + err)
			}
		}
		// rebalance
		newConfig.Rebalance()
		// append new config
		sc.configs = append(sc.configs, newConfig)
	case OprMove:
		moveargs := op.MArgs
		// create a new config
		var newConfig Config
		if len(sc.configs) == 0 {
			newConfig = Config{}
		} else {
			newConfig = sc.configs[len(sc.configs) - 1].Copy()
		}
		newConfig.Num = len(sc.configs)
		// move shard
		if err := newConfig.MoveShard(moveargs.Shard, moveargs.GID); err != OK {
			panic("Move Shard Failed: " + err)
		}
		// append new config
		sc.configs = append(sc.configs, newConfig)
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
	sc.condMap = make(map[int]chan Err)
	sc.dupReq = make(map[int64]int)
	go sc.applyLoop()

	return sc
}
