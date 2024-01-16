package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)



type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClerkId int64
	Seq int
	Leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.ClerkId = nrand()
	ck.Seq = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	DPrintf("[Clerk %v] Query %v CALLED", ck.ClerkId, num)
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	config, _ := ck.Operate(OprQuery, args)
	return config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	ck.Operate(OprJoin, args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.Operate(OprLeave, args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.Operate(OprMove, args)
}

func (ck *Clerk) Operate(op string, args interface{}) (Config, Err) {
	defer func() {
		ck.Seq++
	}()

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			idx := (ck.Leader + i) % len(ck.servers)
			srv := ck.servers[idx]
			reply := GlobalReply{}
			gargs := MakeGlobalArgs(op, args, ck.ClerkId, ck.Seq)

			if ok := srv.Call("ShardCtrler.Operate", gargs, &reply); !ok {
				DPrintf("[Clerk %v] Operate %v %v to %v failed: retry", ck.ClerkId, op, args, idx)
				continue
			}
			if reply.WrongLeader == true {
				DPrintf("[Clerk %v] Operate %v %v failed: wrong leader", ck.ClerkId, op, args)
				continue
			}
			ck.Leader = idx
			if reply.Err == OK {
				DPrintf("[Clerk %v] Operate %v %v success, reply %v", ck.ClerkId, op, args, reply)
				return reply.Config, reply.Err
			}
			if reply.Err == ErrDupReq {
				DPrintf("[Clerk %v] Operate %v %v failed: duplicate request", ck.ClerkId, op, args)
				return reply.Config, reply.Err
			}
			if reply.Err == ErrTimeout {
				DPrintf("[Clerk %v] Operate %v %v failed: timeout", ck.ClerkId, op, args)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if reply.Err == ErrGroupNotExist {
				DPrintf("[Clerk %v] Operate %v %v failed: group not exist", ck.ClerkId, op, args)
				return reply.Config, reply.Err
			}
			if reply.Err == ErrDuplicateGroup {
				DPrintf("[Clerk %v] Operate %v %v failed: duplicate group", ck.ClerkId, op, args)
				return reply.Config, reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func MakeGlobalArgs(op string, args interface{}, clerkId int64, seq int) *GlobalArgs {
	switch op {
	case OprJoin:
		return &GlobalArgs{OpCode: op, JArgs: *args.(*JoinArgs), ClerkId: clerkId, Seq: seq}
	case OprLeave:
		return &GlobalArgs{OpCode: op, LArgs: *args.(*LeaveArgs), ClerkId: clerkId, Seq: seq}
	case OprMove:
		return &GlobalArgs{OpCode: op, MArgs: *args.(*MoveArgs), ClerkId: clerkId, Seq: seq}
	case OprQuery:
		return &GlobalArgs{OpCode: op, QArgs: *args.(*QueryArgs), ClerkId: clerkId, Seq: seq}
	}
	panic("Invalid Op")
}
