package shardctrler

import (
	"log"
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10


var Debug = false
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cg *Config) Copy() Config {
	var newConfig Config
	newConfig.Num = cg.Num
	newConfig.Shards = cg.Shards
	newConfig.Groups = make(map[int][]string)
	for k, v := range cg.Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
}

func (cg *Config) AddGroup(gid int, servers []string) string {
	if _, ok := cg.Groups[gid]; ok {
		return ErrDuplicateGroup
	}
	cg.Groups[gid] = servers
	return OK
}

func (cg *Config) RemoveGroup(gid int) string {
	if _, ok := cg.Groups[gid]; !ok {
		return ErrGroupNotExist
	}
	delete(cg.Groups, gid)
	return OK
}

func (cg *Config) MoveShard(shard int, gid int) string {
	if _, ok := cg.Groups[gid]; !ok {
		return ErrGroupNotExist
	}
	cg.Shards[shard] = gid
	return OK
}

func (cg *Config) Rebalance() {
	// get all groups
	groups := make([]int, 0)
	for k := range cg.Groups {
		groups = append(groups, k)
	}
	if len(groups) == 0 {
		return
	}
	// sort groups by gid in ascending order
	sort.Ints(groups)
	// assign groups to shards
	for i := 0; i < NShards; i++ {
		cg.Shards[i] = groups[i % len(groups)]
	}
}

const (
	OprJoin = "Join"
	OprLeave = "Leave"
	OprMove = "Move"
	OprQuery = "Query"
)

const (
	OK = "OK"
)

type Err string

const (
	ErrWrongLeader = "ErrWrongLeader"
	ErrDupReq = "ErrDuplicateRequest"
	ErrTimeout = "ErrTimeout"
	ErrGroupNotExist = "ErrGroupNotExist"
	ErrDuplicateGroup = "ErrDuplicateGroup"
)

type GlobalArgs struct {
	OpCode string
	QArgs QueryArgs
	JArgs JoinArgs
	LArgs LeaveArgs
	MArgs MoveArgs
	ClerkId int64
	Seq int
}

type GlobalReply struct {
	WrongLeader bool
	Err Err
	Config Config
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
