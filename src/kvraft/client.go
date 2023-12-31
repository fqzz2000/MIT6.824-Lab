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
	DPrintf("[Client] Get(%v)\n", key)
	// You will have to modify this function.
	// retry with timeout
	now := time.Now()
	for time.Since(now) < 3 * time.Second {
		for i := 0; i < len(ck.servers); i++ {
			args := GetArgs{Key: key}
			reply := GetReply{}
			if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); !ok {
				// DPrintf("Get to server %v failed: no reply\n", i)
				continue
			}
			if reply.Err == OK {
				return reply.Value
			}
			if reply.Err == ErrWrongLeader {
				// DPrintf("Get to server %v failed: wrong leader\n", i)
				continue
			}
			if reply.Err == ErrNoKey {
				DPrintf("[Client] Get(%v) ErrNoKey\n", key)
				continue
			}
		}
	}
	DPrintf("[Client] Get(%v) timeout\n", key)
	return ""
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
	// You will have to modify this function.
	DPrintf("[Client] PutAppend(%v, %v, %v)\n", key, value, op)

	// retry with timeout
	now := time.Now()
	for time.Since(now) < 3 * time.Second {
		for i := 0; i < len(ck.servers); i++ {
			args := PutAppendArgs{Key: key, Value: value, Op: op}
			reply := PutAppendReply{}
			if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); !ok {
				// DPrintf("PutAppend to server %v failed: no reply\n", i)
				continue
			}
			if reply.Err == OK {
				return
			}
			if reply.Err == ErrWrongLeader {
				// DPrintf("PutAppend to server %v failed: wrong leader\n", i)
				continue
			}
		}
	}
	DPrintf("[Client] PutAppend(%v, %v, %v) timeout\n", key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
