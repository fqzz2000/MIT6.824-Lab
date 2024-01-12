package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader int
	seq int
	clerkId int64
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
	// You'll have to add code here.
	ck.curLeader = 0
	ck.clerkId = nrand()
	ck.seq = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("[Client%v] Get(%v)\n", ck.clerkId, key)
	// You will have to modify this function.
	// retry with timeout
	for {
		for i := 0; i < len(ck.servers); i++ {
			args := PutAppendArgs{Key: key, Value: "", Op: "Get", ClerkId: ck.clerkId, Seq: ck.seq}
			reply := PutAppendReply{}
			serverIdx := (i + ck.curLeader) % len(ck.servers)
			// if the rpc call failed, need resend
			// for ok := ck.servers[serverIdx].Call("KVServer.PutAppend", &args, &reply); !ok; ok = ck.servers[serverIdx].Call("KVServer.PutAppend", &args, &reply) {
			// 	time.Sleep(10 * time.Millisecond)
			// 	DPrintf("[Client %v] Get to server %v failed: retry\n", ck.clerkId, serverIdx)
			// }
			if ok := ck.servers[serverIdx].Call("KVServer.PutAppend", &args, &reply); !ok {
				DPrintf("[client %v seq %v] Get to server %v failed: retry\n", ck.clerkId, ck.seq,serverIdx)
				continue
			}
			if reply.Err == ErrWrongLeader {
				DPrintf("[client %v seq %v] Get to server %v failed: wrong leader\n", ck.clerkId, ck.seq, serverIdx)
				continue
			}
			if reply.Err == ErrNoKey {
				ck.seq++
				return ""
			}
			if reply.Err == OK {
				ck.seq++
				ck.curLeader = serverIdx
				DPrintf("[client %v seq %v] Get to server %v success: key = %v;value = %v\n", ck.clerkId, ck.seq, serverIdx,args.Key, reply.Value)
				return reply.Value
			}
			if reply.Err == ErrDupReq {
				DPrintf("[client %v seq %v] Get to server %v failed: duplicate request\n", ck.clerkId, ck.seq, serverIdx)
				ck.seq++
				return ""
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// TODO: need a cache to acce
	// You will have to modify this function.
	DPrintf("[client %v seq %v] PutAppend(%v, %v, %v)\n", ck.clerkId, ck.seq, key, value, op)

	// retry with timeout
	for {
		for i := 0; i < len(ck.servers); i++ {
			args := PutAppendArgs{Key: key, Value: value, Op: op, ClerkId: ck.clerkId, Seq: ck.seq}
			reply := PutAppendReply{}
			serverIdx := (i + ck.curLeader) % len(ck.servers)
			// for ok := ck.servers[serverIdx].Call("KVServer.PutAppend", &args, &reply); !ok; ok = ck.servers[serverIdx].Call("KVServer.PutAppend", &args, &reply) {
			// 	time.Sleep(10 * time.Millisecond)
			// 	DPrintf("[Client %v] PutAppend to server %v failed: retry\n", ck.clerkId, i)
			// }
			if ok := ck.servers[serverIdx].Call("KVServer.PutAppend", &args, &reply); !ok {
				DPrintf("[client %v seq %v] PutAppend to server %v failed: retry\n", ck.clerkId, ck.seq, serverIdx)
				continue
			}
			if reply.Err == OK {
				DPrintf("[client %v seq %v] PutAppend to server %v success OP: %v, Key=%v, Value %v\n", ck.clerkId, ck.seq, serverIdx, op, key, value)
				ck.curLeader = serverIdx
				ck.seq++
				return
			}
			if reply.Err == ErrWrongLeader {
				DPrintf("[client %v seq %v] PutAppend to server %v failed: wrong leader\n", ck.clerkId, ck.seq, serverIdx)
				continue
			}	
			if reply.Err == ErrDupReq {
				DPrintf("[client %v seq %v]PutAppend to server %v failed: duplicate request\n", ck.clerkId, ck.seq, serverIdx)
				ck.seq++
				return
			}
			if reply.Err == ErrNoKey {
				DPrintf("[client %v seq %v]PutAppend to server %v failed: no key\n", ck.clerkId, ck.seq, serverIdx)
				ck.seq++
				return
			}
			
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
