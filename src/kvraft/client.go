package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

// import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID    int64
	sequence    int
	leaderIndex int //record recent leader
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
	ck.clientID = nrand()
	ck.sequence = 0
	ck.leaderIndex = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{key, ck.clientID, ck.sequence}
	ck.sequence++
	i := ck.leaderIndex
	for {
		reply := GetReply{}
		if ok := ck.servers[i].Call("RaftKV.Get", &args, &reply); ok {
			// fmt.Println("Reply to Get:", reply)
			if !reply.WrongLeader && reply.Err == OK {
				// fmt.Println("Get Value, ", reply.Value)
				return reply.Value
			} else {
				// fmt.Println("Get Error", reply.Err)
			}
		}
		i = (i + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, ck.clientID, ck.sequence}
	ck.sequence++

	i := ck.leaderIndex
	num_servers := len(ck.servers)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		// fmt.Println("Reply to Put:", reply)
		if ok {
			if !reply.WrongLeader && reply.Err == OK {
				// fmt.Println("PutAppend Success")
				break
			} else {
				// fmt.Println("Failed to Put", reply.Err)
			}
		}
		i = (i + 1) % num_servers
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
