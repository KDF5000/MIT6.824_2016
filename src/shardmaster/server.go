package shardmaster

import (
	"container/list"
	"encoding/gob"
	"fmt"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	notify map[int][]chan Op
	Recv   chan raft.ApplyMsg
	Msgs   *list.List
	msgMu  sync.Mutex

	lastIncludeIndex int
	lastIncludeTerm  int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Client   int64
	Sequence int
	// Config   Config
	Servers map[int][]string //for join
	GIDS    []int            //for leave
	//for move
	Shard int
	GID   int
	//query
	Num  int
	Type string
	Err  Err
}

type OpReply struct {
	Config      Config
	Type        string
	WrongLeader bool
	Err         Err
}

func (sm *ShardMaster) startRequest(op Op, reply *OpReply) {
	sm.mu.Lock()
	// fmt.Println("Join:", newConfig, sm.rf.GetRole())
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		// fmt.Println("Join: WrongLeader")
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	fmt.Printf("%d receive request %s %d:%d\n", sm.me, op.Type, op.Client, op.Sequence)
	if _, ok := sm.notify[index]; !ok {
		sm.notify[index] = make([]chan Op, 0)
	}
	indexNotify := make(chan Op, 0)
	sm.notify[index] = append(sm.notify[index], indexNotify)
	sm.mu.Unlock()

	//waiting for notification
	var executeOp Op
	notified := false
	for {
		select {
		case executeOp = <-indexNotify:
			notified = true
			break
		case <-time.After(10 * time.Millisecond):
			sm.mu.Lock()
			currentTerm, _ := sm.rf.GetState()
			if term != currentTerm {
				if sm.lastIncludeIndex < index {
					reply.WrongLeader = true
					delete(sm.notify, index)
					sm.mu.Unlock()
					return
				}
			}
			sm.mu.Unlock()
		}
		if notified {
			break
		}
	}
	reply.WrongLeader = false
	if executeOp.Client != op.Client || executeOp.Sequence != op.Sequence {
		reply.WrongLeader = true
		reply.Err = "FailCommit"
	} else {
		reply.Err = OK
		reply.Config = executeOp.Config
		reply.Type = executeOp.Type
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	lastConfig := sm.configs[len(sm.configs)-1]
	var newConfig Config
	newConfig.Num = lastConfig.Num + 1
	newConfig.Groups = make(map[int][]string, 0)
	gids := make([]int, 0)
	//生成新的groups
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v //是否需要重新创建一个slice
		gids = append(gids, k)
	}
	for k, v := range args.Servers {
		if _, ok := newConfig.Groups[k]; !ok {
			gids = append(gids, k)
		}
		newConfig.Groups[k] = v //是否需要重新创建一个slice
	}
	//重新均衡的分配shards
	shardsNum := len(newConfig.Shards)
	for i := 0; i < shardsNum; {
		for j := 0; j < len(gids) && i < shardsNum; j++ {
			newConfig.Shards[i] = gids[j]
			i++
		}
	}
	newConfig.Num = lastConfig.Num + 1
	sm.mu.Unlock()
	//start request
	op := Op{args.Client, args.Sequence, newConfig, "Join", OK}
	var opReply OpReply
	sm.startRequest(op, &opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	return
}

func (sm *ShardMaster) applyJoin(op Op) {
	sm.mu.Lock()
	lastConfig := sm.configs[len(sm.configs)-1]
	var newConfig Config
	newConfig.Num = lastConfig.Num + 1
	newConfig.Groups = make(map[int][]string, 0)
	gids := make([]int, 0)
	//生成新的groups
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v //是否需要重新创建一个slice
		gids = append(gids, k)
	}
	for k, v := range op.Servers {
		if _, ok := newConfig.Groups[k]; !ok {
			gids = append(gids, k)
		}
		newConfig.Groups[k] = v //是否需要重新创建一个slice
	}
	//重新均衡的分配shards
	shardsNum := len(newConfig.Shards)
	for i := 0; i < shardsNum; {
		for j := 0; j < len(gids) && i < shardsNum; j++ {
			newConfig.Shards[i] = gids[j]
			i++
		}
	}
	sm.configs = append(sm.configs, newConfig)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	lastConfig := sm.configs[len(sm.configs)-1]
	var newConfig Config
	newConfig.Groups = make(map[int][]string, 0)
	//生成新的groups
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v //是否需要重新创建一个slice
	}
	//delete gids
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}
	gids := make([]int, 0)
	for k, _ := range newConfig.Groups {
		gids = append(gids, k)
	}
	//重新均衡的分配shards
	shardsNum := len(newConfig.Shards)
	for i := 0; i < shardsNum; {
		for j := 0; j < len(gids) && i < shardsNum; j++ {
			newConfig.Shards[i] = gids[j]
			i++
		}
	}
	newConfig.Num = lastConfig.Num + 1
	// fmt.Println(sm.me, "Leave:", newConfig)
	sm.mu.Unlock()
	//start request
	op := Op{args.Client, args.Sequence, newConfig, "Leave", OK}
	var opReply OpReply
	sm.startRequest(op, &opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	lastConfig := sm.configs[len(sm.configs)-1]
	var newConfig Config
	newConfig.Groups = make(map[int][]string, 0)
	//生成新的groups
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v //是否需要重新创建一个slice
	}
	//新的shards
	for k, v := range lastConfig.Shards {
		newConfig.Shards[k] = v
	}
	newConfig.Num = lastConfig.Num + 1
	//Move
	newConfig.Shards[args.Shard] = args.GID
	sm.mu.Unlock()
	//start request using raft
	op := Op{args.Client, args.Sequence, newConfig, "Move", OK}
	var opReply OpReply
	sm.startRequest(op, &opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var newConfig Config
	sm.mu.Lock()
	if args.Num == -1 || args.Num > len(sm.configs)-1 {
		newConfig = sm.configs[len(sm.configs)-1]
	} else {
		newConfig = sm.configs[args.Num]
	}
	sm.mu.Unlock()
	op := Op{args.Client, args.Sequence, newConfig, "Query", OK}
	var opReply OpReply
	sm.startRequest(op, &opReply)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	reply.Config = opReply.Config
	return
}

// func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
// 	// Your code here.
// 	lastConfig := sm.configs[len(sm.configs)-1]

// 	//start request
// 	if _, isLeader := sm.rf.GetState(); !isLeader {
// 		reply.WrongLeader = true
// 		return
// 	}
// 	reply.WrongLeader = false
// 	reply.Err = OK

// 	if args.Num == -1 {
// 		reply.Config = lastConfig
// 		return
// 	}
// 	//traverse the configs to find the config with Num
// 	for _, v := range sm.configs {
// 		if v.Num == args.Num {
// 			reply.Config = v
// 			return
// 		}
// 	}
// 	reply.Config = lastConfig
// 	return
// }

//apply request
func (sm *ShardMaster) applyCommand(msg raft.ApplyMsg) {
	sm.lastIncludeIndex = msg.Index
	sm.lastIncludeTerm = msg.Term
	index := msg.Index

	op := msg.Command.(Op)
	if op.Type != "Query" {
		sm.configs = append(sm.configs, op.Config)
	}
	// switch op.Type {
	// case "Join":
	// 	sm.configs = append(sm.configs, op.Config)
	// case "Leave":
	// 	//do something

	// case "Move":
	// 	//do something
	// case "Query":
	// 	//do something
	// }
	if _, ok := sm.notify[index]; !ok {
		// fmt.Println(sm.me, "No request needed to be notified!")
		return
	}
	for _, c := range sm.notify[index] {
		sm.mu.Unlock()
		c <- op
		sm.mu.Lock()
	}
	delete(sm.notify, index)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.notify = make(map[int][]chan Op, 0)
	sm.Msgs = list.New()
	sm.Recv = make(chan raft.ApplyMsg, 0)

	go func() {
		for {
			var (
				recvChan chan raft.ApplyMsg
				recvVal  raft.ApplyMsg
			)
			if sm.Msgs.Len() > 0 {
				recvChan = sm.Recv
				recvVal = sm.Msgs.Front().Value.(raft.ApplyMsg)
			}
			select {
			case msg := <-sm.applyCh:
				sm.Msgs.PushBack(msg)
				// fmt.Println(sm.me, "Receive msg from raft group", msg)
			case recvChan <- recvVal:
				sm.Msgs.Remove(sm.Msgs.Front())
			}
		}
	}()

	go func() {
		for {
			select {
			case msg := <-sm.Recv:
				//apply
				// fmt.Println(sm.me, "need to apply command")
				sm.mu.Lock()
				sm.applyCommand(msg)
				sm.mu.Unlock()
			}
		}
	}()
	return sm
}
