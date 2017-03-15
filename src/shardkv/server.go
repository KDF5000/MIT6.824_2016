package shardkv

// import "shardmaster"
import (
	"container/list"
	"encoding/gob"
	// "fmt"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client   int64
	Sequence int
	Key      string
	Value    string
	Type     string //Get or PutAppend
	Err      Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string
	executedSeq map[int64]int
	notify      map[int][]chan Op

	Recv  chan raft.ApplyMsg
	Msgs  *list.List
	msgMu sync.Mutex

	lastIncludeIndex int
	lastIncludeTerm  int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// fmt.Println("Begin to Get")
	op := Op{args.Client, args.Sequence, args.Key, "", "Get", ""}
	kv.mu.Lock()
	if _, ok := kv.executedSeq[args.Client]; !ok {
		kv.executedSeq[args.Client] = -1
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	}

	if _, ok := kv.notify[index]; !ok {
		kv.notify[index] = make([]chan Op, 0)
	}
	indexNotify := make(chan Op)
	kv.notify[index] = append(kv.notify[index], indexNotify)
	kv.mu.Unlock()

	var executeOp Op
	notified := false
	for {
		select {
		case executeOp = <-indexNotify:
			notified = true
			break
		case <-time.After(10 * time.Millisecond):
			kv.mu.Lock()
			currentTerm, _ := kv.rf.GetState()
			if term != currentTerm {
				if kv.lastIncludeIndex < index {
					reply.WrongLeader = true
					delete(kv.notify, index)
					kv.mu.Unlock()
					return
				}
			}
			kv.mu.Unlock()
		}
		if notified {
			break
		}
	}
	reply.WrongLeader = false
	if executeOp.Client != op.Client || executeOp.Sequence != op.Sequence {
		reply.Err = "FailCommit"
	} else {
		if executeOp.Err == ErrNoKey {
			reply.Err = ErrNoKey
		} else {
			// fmt.Println("Get Result", executeOp)
			reply.Err = OK
			reply.Value = executeOp.Value
		}
	}
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Printf("Server %d Receive Put request %s=%s\n", kv.me, args.Key, args.Value)
	op := Op{args.Client, args.Sequence, args.Key, args.Value, args.Op, ""}
	kv.mu.Lock()
	if _, ok := kv.executedSeq[args.Client]; !ok {
		kv.executedSeq[args.Client] = -1
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	}

	if _, ok := kv.notify[index]; !ok {
		kv.notify[index] = make([]chan Op, 0)
	}
	indexNotify := make(chan Op)
	kv.notify[index] = append(kv.notify[index], indexNotify)
	kv.mu.Unlock()
	// fmt.Printf("Server %d wait for notify: %s\n", kv.me, args.Key)
	var executeOp Op
	notified := false
	for {
		select {
		case executeOp = <-indexNotify:
			notified = true
			break
		case <-time.After(10 * time.Millisecond):
			kv.mu.Lock()
			currentTerm, _ := kv.rf.GetState()
			if term != currentTerm {
				if kv.lastIncludeIndex < index {
					reply.WrongLeader = true
					delete(kv.notify, index)
					kv.mu.Unlock()
					return
				}
			}
			kv.mu.Unlock()
		}
		if notified {
			// fmt.Printf("Server %d receive notify key %s\n", kv.me, executeOp.Key)
			break
		}
	}
	reply.WrongLeader = false
	if executeOp.Client != op.Client || executeOp.Sequence != op.Sequence {
		reply.Err = "FailCommit"
	} else {
		reply.Err = OK
	}
}

func (kv *ShardKV) applyCommand(msg raft.ApplyMsg) {
	kv.lastIncludeIndex = msg.Index
	kv.lastIncludeTerm = kv.rf.GetTerm(kv.lastIncludeIndex)
	index := msg.Index
	op := msg.Command.(Op)
	clientId := op.Client
	opSequence := op.Sequence

	res := Op{clientId, opSequence, op.Key, op.Value, op.Type, op.Err}
	if _, ok := kv.executedSeq[clientId]; !ok {
		kv.executedSeq[clientId] = -1
	}

	if opSequence > kv.executedSeq[clientId] {
		switch op.Type {
		case "Get":
			v, ok := kv.data[op.Key]
			if !ok {
				res.Err = ErrNoKey
			} else {
				res.Value = v
			}
			// fmt.Println("Apply Get", op.Key, v)
		case "Put":
			// fmt.Println("Apply Put", op.Key, op.Value)
			kv.data[op.Key] = op.Value
		case "Append":
			if _, ok := kv.data[op.Key]; ok {
				kv.data[op.Key] += op.Value
			} else {
				kv.data[op.Key] = op.Value
			}
		}
		kv.executedSeq[clientId] = opSequence
	} else {
		if op.Type == "Get" {
			v, ok := kv.data[op.Key]
			if !ok {
				res.Err = ErrNoKey
			} else {
				res.Value = v
			}
		}
	}
	if _, ok := kv.notify[index]; !ok {
		return
	}
	for _, c := range kv.notify[index] {
		kv.mu.Unlock()
		// fmt.Printf("Server %d send notify key %s, Data:%v\n", kv.me, res.Key, kv.data)
		c <- res
		kv.mu.Lock()
	}
	delete(kv.notify, index)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string, 0)
	kv.Msgs = list.New()
	kv.Recv = make(chan raft.ApplyMsg)
	kv.notify = make(map[int][]chan Op, 0)
	kv.executedSeq = make(map[int64]int, 0)

	kv.lastIncludeTerm = 0
	kv.lastIncludeIndex = 0

	//waiting for applymsg
	go func() {
		for {
			var (
				recvChan chan raft.ApplyMsg
				recvVal  raft.ApplyMsg
			)
			if kv.Msgs.Len() > 0 {
				recvChan = kv.Recv
				recvVal = kv.Msgs.Front().Value.(raft.ApplyMsg)
			}
			select {
			case msg := <-kv.applyCh:
				kv.Msgs.PushBack(msg)
			case recvChan <- recvVal:
				kv.Msgs.Remove(kv.Msgs.Front())
			}
		}
	}()

	go func() {
		for {
			select {
			case msg := <-kv.Recv:
				kv.mu.Lock()
				kv.applyCommand(msg)
				kv.mu.Unlock()
			}
		}
	}()
	return kv
}
