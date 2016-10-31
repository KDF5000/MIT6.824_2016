package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	// "fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for description of what
	// state a Raft server must maintain.
	//Persistent state on all servers
	CurrentTerm int //当前term
	VotedFor    int //投票
	Log         []interface{}

	//volatitle on all servers
	CommitIndex int //index of hishest log entry
	LastApplied int //index of highest log entry applied to state machine

	//volatitle state on leaders
	NextIndex  []int //initialized to leader last log index + 1
	MatchIndex []int //initialized to 0

	//custom element
	Role        int       // leader， candidate, follower
	ToFollower  chan bool //channel to follower
	HeartBeatCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = (rf.Role == LEADER)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //candidate`s term
	CandidateId  int //cnadidate`s requesting vote
	LastLogIndex int //index of candidate`s last log entry
	LastLogTerm  int //term of candidate`s last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  //currentTerm for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int           //leader`s term
	LeaderId     int           //so follower can redirect clients
	PrevLogIndex int           //index of log entry immediately preceding newe ones
	PreLogTerm   int           //Term of prevLogIndex entry
	Entries      []interface{} //log entries to store(empty for heartbeat)
	LeaderCommit int           //leader`s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm for leader to update itself
	Success bool //true if follower cotained entry matching prevLogIndex and PrevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.Role = FOLLOWER
		go func() {
			rf.ToFollower <- true
		}()
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		go func() {
			rf.HeartBeatCh <- true
		}()
	} else {
		reply.VoteGranted = false
	}

	//stay follower
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println(rf.me, "Recceived HeartBeat from ", args.LeaderId)
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm

	if rf.CurrentTerm > args.Term {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	go func() { rf.HeartBeatCh <- true }()
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.Role = FOLLOWER
		go func() {
			rf.ToFollower <- true
		}()
	}

	reply.Term = rf.CurrentTerm
	reply.Success = true
	rf.mu.Unlock()

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) GetRole() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Role
}

func (rf *Raft) followerAction() {
	rand.Seed(time.Now().Unix() + int64(rf.me))
	millSeconds := time.Duration(rand.Intn(150) + 150)
	// fmt.Println(millSeconds * time.Millisecond)
	t := time.NewTimer(millSeconds * time.Millisecond)
	for {
		select {
		case <-t.C:
			//timeout, become candidate
			rf.becomeCandidate()
			return
		case <-rf.ToFollower:
			continue
		case <-rf.HeartBeatCh:
			//reset timeout
			millSeconds = time.Duration(rand.Intn(150) + 150)
			t.Reset(millSeconds * time.Millisecond)
		}
	}
}

func (rf *Raft) becomeCandidate() {
	// fmt.Println(rf.me, " Becomes Candidate!")
	rf.mu.Lock()
	rf.Role = CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.me //vote for itself
	rf.mu.Unlock()
}

func (rf *Raft) candidateAction() {
	// fmt.Println(rf.me, " In Candidate")
	totalVotes := 1 // one vote for itself
	collectVote := make(chan bool)

	rf.mu.Lock()
	args := RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.me}
	rf.mu.Unlock()
	rand.Seed(time.Now().Unix() + int64(rf.me))
	// electionTimeout := time.NewTimer(time.Duration(rand.Intn(150) + 150))
	electionTimeout := time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	for i := range rf.peers {
		if i != rf.me {
			go func(index int) {
				reply := &RequestVoteReply{VoteGranted: false}
				ok := false
				timeout := time.Now()
				//retry RPC call every 10ms, timeout is 200ms
				for rf.GetRole() == CANDIDATE && rf.CurrentTerm == args.Term && time.Since(timeout).Seconds() < 0.2 {
					retryTime := 50 * time.Millisecond
					checkTimeOut := time.NewTimer(time.Duration(10) * time.Millisecond)
					go func() {
						ok = rf.sendRequestVote(index, args, reply)
					}()
					// for j := 1; (j < 4) && (!ok1); j++ {
					// 		<-t.C
					// 		t.Reset(10 * time.Millisecond)
					// 	}
					// 	<-t.C
					for !ok {
						select {
						case <-time.After(retryTime):
							break
						case <-checkTimeOut.C:
							// fmt.Println("Send RequestVote to ", index)
							if ok {
								break
							} else {
								checkTimeOut.Reset(time.Duration(10) * time.Millisecond)
							}
						}
					}
					if ok {
						break
					}
				}
				// RPC might fail, election timeout and may re-elect.
				if !ok {
					return
				}
				if reply.VoteGranted {
					<-collectVote
				} else {
					rf.mu.Lock()
					if reply.Term > rf.CurrentTerm {
						// if some server has higher term, then update currentTerm, changes to follower, and save state to persist.
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1
						rf.Role = FOLLOWER
						rf.mu.Unlock()
						go func() { rf.ToFollower <- true }()
					} else {
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
	//collect vote
	for rf.GetRole() == CANDIDATE {
		select {
		case collectVote <- true:
			totalVotes++
			//becomeleader
			// fmt.Println(rf.me, "Total Vote", totalVotes)
			if totalVotes > len(rf.peers)/2 {
				rf.mu.Lock()

				rf.Role = LEADER
				close(collectVote)
				// fmt.Println(rf.me, "become leader")
				rf.mu.Unlock()
				rf.sendHeartBeat()
				return
			}
		case <-electionTimeout.C:
			//restart election
			rf.becomeCandidate()
			return
		case <-rf.HeartBeatCh:
			continue
		case <-rf.ToFollower:
			// fmt.Println(rf.me, "Return to Follower from Candidate")
			return
		}
	}
}
func (rf *Raft) sendHeartBeat() {
	for i := range rf.peers {
		if i != rf.me {
			go func(index int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{Term: rf.CurrentTerm, LeaderId: rf.me}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(index, args, reply)
				if ok {
					rf.mu.Lock()
					// update currentTerm according to follower's term, and might transfer to follower.
					// or update nextindex according to conflictentry sent by follower.
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1
						rf.Role = FOLLOWER
						rf.mu.Unlock()
						go func() { rf.ToFollower <- true }()
						// go rf.persist()
					} else {
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}
func (rf *Raft) leaderAction() {
	// fmt.Println("Leader", rf.me, "send heartbeat")
	// rf.sendHeartBeat()
	rf.mu.Lock()
	currentTerm := rf.CurrentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go func(index int) {
				timeout := 60 * time.Millisecond
				for rf.GetRole() == LEADER && rf.CurrentTerm == currentTerm {
					// if leader transform to follower while entering makeAppendEntriesArgs function,
					// then do not send that request and becomes to follower
					select {
					case <-time.After(timeout):
						// timeout = 60 * time.Millisecond
						rf.mu.Lock()
						args := AppendEntriesArgs{Term: rf.CurrentTerm, LeaderId: rf.me}
						rf.mu.Unlock()
						reply := &AppendEntriesReply{}

						go func() {
							if ok := rf.sendAppendEntries(index, args, reply); ok {

								rf.mu.Lock()
								if reply.Term > rf.CurrentTerm {
									rf.CurrentTerm = reply.Term
									rf.VotedFor = -1
									rf.Role = FOLLOWER
									rf.mu.Unlock()
									go func() { rf.ToFollower <- true }()
									// go rf.persist()
								} else {
									rf.mu.Unlock()
								}
							}
						}()
						//time.Sleep(60*time.Millisecond)
					}
				}
			}(i)
		}
	}
	for rf.GetRole() == LEADER {
		select {
		case <-rf.ToFollower:
			return
		case <-rf.HeartBeatCh:
			continue
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here.
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.HeartBeatCh = make(chan bool)
	rf.ToFollower = make(chan bool)

	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.Role = FOLLOWER

	go func() {
		for {
			// fmt.Println("for")
			switch rf.GetRole() {
			case FOLLOWER:
				// fmt.Println(rf.me, " Role: Follower")
				rf.followerAction()
			case CANDIDATE:
				// fmt.Println(rf.me, " Role: Candidate")
				rf.candidateAction()
			case LEADER:
				// fmt.Println(rf.me, " Role: Leader")
				rf.leaderAction()
			}
		}
	}()
	return rf
}
