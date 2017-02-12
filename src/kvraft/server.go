package raftkv

import (
	"container/list"
	"encoding/gob"
	// "fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	//ensure sequence consistence
	Client   int64
	Sequence int
	Key      string
	Value    string
	Type     string //Get Put PutAppend
	Err      Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string
	executedSeq map[int64]int
	notify      map[int][]chan Op

	Recv  chan raft.ApplyMsg
	Msgs  *list.List
	msgMu sync.Mutex
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{args.Client, args.Sequence, args.Key, "", "Get", ""}

	DPrintf("Begin to Get", op)
	kv.mu.Lock()
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
	DPrintf("Leader %d Put Client %d notification in index %d\n", kv.me, op.Client, index)
	kv.mu.Unlock()

	var executeOp Op
	notified := false
	//wait for commiting op
	for {
		select {
		case executeOp = <-indexNotify:
			notified = true
			break
		case <-time.After(10 * time.Millisecond):
			kv.mu.Lock()
			currentTerm, _ := kv.rf.GetState()
			if term != currentTerm {
				reply.WrongLeader = true
				delete(kv.notify, index)
				kv.mu.Unlock()
				return
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
			// DPrintf("Get Value", executeOp.Value)
			reply.Err = OK
			reply.Value = executeOp.Value
		}
	}
	DPrintf("Get reply", reply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Client, args.Sequence, args.Key, args.Value, args.Op, ""}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.WrongLeader = true
		DPrintf("%d is not leader\n", kv.me)
		return
	}
	//
	DPrintf("Begin to PutAppend", op)
	if _, ok := kv.notify[index]; !ok {
		kv.notify[index] = make([]chan Op, 0)
	}
	indexNotify := make(chan Op)
	kv.notify[index] = append(kv.notify[index], indexNotify)
	DPrintf("Leader %d Put Client %d notification in Index %d \n", kv.me, op.Client, index)
	kv.mu.Unlock()

	var executeOp Op
	notified := false
	//wait for commiting op
	for {
		select {
		case executeOp = <-indexNotify:
			notified = true
			// DPrintf("Receive Notify")
			break
		case <-time.After(100 * time.Millisecond):
			kv.mu.Lock()
			currentTerm, _ := kv.rf.GetState()
			if term != currentTerm {
				reply.WrongLeader = true
				delete(kv.notify, index)
				kv.mu.Unlock()
				return
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
		reply.Err = OK
	}
	DPrintf("PutApend Reply", reply)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) applyCommand(msg raft.ApplyMsg) {
	// Index       int
	// Term        int
	// Command     interface{}
	// DPrintf("applych:", msg)
	index := msg.Index
	// term := msg.Term
	op := msg.Command.(Op)
	clientId := op.Client
	opSequence := op.Sequence
	DPrintf("%d apply command, ", kv.me, msg)
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
			// DPrintf("Get Value", res.Err, res.Value)
		case "Put":
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
	DPrintf("Server ", kv.me, "Data:", kv.data)
	if _, ok := kv.notify[index]; !ok {
		DPrintf("%d %d Empty", kv.me, index, kv.notify)
		return
	}
	for _, c := range kv.notify[index] {
		kv.mu.Unlock()
		c <- res
		kv.mu.Lock()
	}
	delete(kv.notify, index)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.data = make(map[string]string, 0)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.Msgs = list.New()
	kv.Recv = make(chan raft.ApplyMsg)

	kv.notify = make(map[int][]chan Op, 0)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.executedSeq = make(map[int64]int, 0)

	// recvChan := make(chan raft.ApplyMsg)
	// go func() {
	// 	//收集消息
	// 	for {
	// 		log.Println(kv.me, "collect select")
	// 		var (
	// 			recvMsg  raft.ApplyMsg
	// 			recvChan chan raft.ApplyMsg
	// 		)
	// 		// if kv.Msgs.Len() > 0 {
	// 		// 	recvChan = kv.Recv
	// 		// 	recvMsg = kv.Msgs.Front().Value.(raft.ApplyMsg)
	// 		// 	log.Println(i, kv.me, " Pop Msg", recvMsg)
	// 		// }

	// 		select {
	// 		case msg := <-kv.applyCh:
	// 			log.Println(kv.me, "recv msg", msg)
	// 			// kv.msgMu.Lock()
	// 			// kv.Msgs.PushBack(msg)
	// 			// kv.msgMu.Unlock()
	// 			// default:
	// 			// if kv.Msgs.Len() > 0 {
	// 			// 	recvMsg := kv.Msgs.Front().Value.(raft.ApplyMsg)
	// 			// 	log.Println(kv.me, " Pop Msg", recvMsg)
	// 			// 	// kv.mu.Lock()
	// 			// 	// kv.applyCommand(recvMsg)
	// 			// 	// kv.mu.Unlock()
	// 			// 	recvChan <- recvMsg
	// 			// 	log.Println(kv.me, " remove Msg", recvMsg)
	// 			// 	kv.Msgs.Remove(kv.Msgs.Front())
	// 			// }
	// 		}
	// 		// case recvChan <- recvMsg:
	// 		// 	log.Println(kv.me, " push to recvChan channel", recvMsg)
	// 		// 	<-recvChan
	// 		// 	// kv.mu.Lock()
	// 		// 	// kv.applyCommand(recvMsg)
	// 		// 	// kv.mu.Unlock()
	// 		// 	kv.Msgs.Remove(kv.Msgs.Front())
	// 		// 	// default:
	// 		// }
	// 	}
	// }()
	// go func() {
	// 	for {
	// 		if kv.Msgs.Len() > 0 {
	// 			// kv.msgMu.Lock()
	// 			// recvMsg := kv.Msgs.Front().Value.(raft.ApplyMsg)
	// 			// kv.Msgs.Remove(kv.Msgs.Front())
	// 			// log.Println(kv.me, " remove Msg", recvMsg)
	// 			// kv.msgMu.Unlock()

	// 			// kv.mu.Lock()
	// 			// kv.applyCommand(recvMsg)
	// 			// kv.mu.Unlock()
	// 		}
	// 		// select {
	// 		// case msg := <-recvChan:
	// 		// 	log.Println("apply msg")
	// 		// 	kv.mu.Lock()
	// 		// 	kv.applyCommand(msg)
	// 		// 	kv.mu.Unlock()
	// 		// 	// default:
	// 		// }
	// 	}
	// }()
	// go func() {
	// 	for {
	// 		// log.Println(kv.me, " consume msg")
	// 		if kv.Msgs.Len() > 0 {
	// 			recvMsg := kv.Msgs.Front().Value.(raft.ApplyMsg)
	// 			// log.Println(kv.me, " consume msg ", recvMsg)
	// 			kv.mu.Lock()
	// 			// log.Printf("%d apply msg \n", kv.me)
	// 			//DPrintf(kv.me, " applying command at index:", msg.Index)
	// 			// kv.appliedEntry++
	// 			kv.applyCommand(recvMsg)
	// 			kv.mu.Unlock()
	// 			kv.Msgs.Remove(kv.Msgs.Front())
	// 		}
	// 		// select {
	// 		// case applyMsg := <-recvChannel:
	// 		// 	kv.mu.Lock()
	// 		// 	// log.Printf("%d apply msg \n", kv.me)
	// 		// 	//DPrintf(kv.me, " applying command at index:", msg.Index)
	// 		// 	// kv.appliedEntry++
	// 		// 	kv.applyCommand(applyMsg)
	// 		// 	kv.mu.Unlock()
	// 		// }
	// 	}
	// }()

	go func() {
		i := 1
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
				// log.Printf("%d push new msg, kv.Msg:%d", i, kv.Msgs.Len())
			case recvChan <- recvVal:
				kv.Msgs.Remove(kv.Msgs.Front())
				// log.Printf("%d remove msg", i)
				// default:
			}
			i++
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

	//snapshot
	go func() {
		if maxraftstate <= 0 {
			return
		}
		for {
			select {
			case <-time.After(5 * time.Millisecond):

			}
		}
	}()

	return kv
}
