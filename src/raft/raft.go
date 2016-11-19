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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

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
	Term        int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int //the term number when the log was received by the leader
	Index   int //the position in the log
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
	CurrentTerm int //current term
	VotedFor    int //the server voted for
	Log         []LogEntry

	//volatitle on all servers
	CommitIndex int //index of hishest log entry
	LastApplied int //index of highest log entry applied to state machine

	//volatitle state on leaders
	NextIndex  []int //initialized to leader last log index + 1
	MatchIndex []int //for each server, index of highest log entry known to be replicated on server,initialized to 0

	//custom element
	Role        int       // leader， candidate, follower
	ToFollower  chan bool //channel to follower
	HeartBeatCh chan bool
	ApplyCh     chan ApplyMsg
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	// fmt.Println("Persist:", rf.Log)
	for _, entry := range rf.Log {
		e.Encode(entry)
	}
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	firstIndex := true
	for {
		var entry LogEntry
		err := d.Decode(&entry)
		if err != nil {
			break
		}
		if firstIndex {
			rf.Log = make([]LogEntry, 0)
			firstIndex = false
		}
		// fmt.Println("Recovery:", entry)
		rf.Log = append(rf.Log, entry)
	}

	rf.mu.Unlock()
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
	Term         int        //leader`s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding newe ones
	PrevLogTerm  int        //Term of prevLogIndex entry
	Entries      []LogEntry //log entries to store(empty for heartbeat)
	LeaderCommit int        //leader`s commitIndex
}

type AppendEntriesReply struct {
	ConflictEntry int
	Term          int  //currentTerm for leader to update itself
	Success       bool //true if follower cotained entry matching prevLogIndex and PrevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		rf.mu.Unlock()
		reply.VoteGranted = false
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.Role = FOLLOWER
		go func() { rf.ToFollower <- true }()
	}

	reply.Term = rf.CurrentTerm
	lastIndex := rf.Log[len(rf.Log)-1].Index
	lastTerm := rf.Log[len(rf.Log)-1].Term

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		((args.LastLogTerm > lastTerm) || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex)) {
		rf.VotedFor = args.CandidateId
		go func() { rf.HeartBeatCh <- true }()
		reply.VoteGranted = true
		rf.mu.Unlock()
		// go rf.persist()
	} else {
		reply.VoteGranted = false
		rf.mu.Unlock()
	}
	// fmt.Println(rf.me, "vote for ", args.CandidateId, reply.VoteGranted)
}

func (rf *Raft) applyLogEntries() {
	rf.mu.Lock()
	for rf.CommitIndex > rf.LastApplied {
		rf.LastApplied++
		base := rf.Log[0].Index
		msg := ApplyMsg{}
		msg.Index = rf.LastApplied
		msg.Term = rf.Log[rf.LastApplied-base].Term

		if rf.LastApplied < base {
			fmt.Println("Failed LastApplied:", len(rf.Log), rf.LastApplied, rf.CommitIndex)
		}
		msg.Command = rf.Log[rf.LastApplied-base].Command

		rf.ApplyCh <- msg
		// fmt.Println(rf.me, "send msg ", msg, "to applych successfully")
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println(rf.me, "Recceived AppendEntries from ", args.LeaderId)
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	reply.ConflictEntry = args.PrevLogIndex + 1

	if rf.CurrentTerm > args.Term {
		rf.mu.Unlock()
		reply.Success = false
		// fmt.Println(rf.me, "reject append ", rf.CurrentTerm)
		return
	}

	change := false
	go func() { rf.HeartBeatCh <- true }()
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.Role = FOLLOWER
		change = true
		go func() { rf.ToFollower <- true }()
	}

	reply.Term = rf.CurrentTerm
	base := rf.Log[0].Index
	if args.PrevLogIndex < base || args.PrevLogIndex >= base+len(rf.Log) || args.PrevLogTerm != rf.Log[args.PrevLogIndex-base].Term {
		reply.Success = false
		// fmt.Println(rf.me, "Refuse ", args.PrevLogIndex, len(rf.Log), args.PrevLogTerm)
		var conflict int
		if args.PrevLogIndex < base {
			conflict = 1
		} else if base+len(rf.Log) <= args.PrevLogIndex {
			conflict = base + len(rf.Log)
		} else {
			conflict = args.PrevLogIndex
			var conflictTerm = rf.Log[conflict-base].Term
			fi := conflict
			for ; fi > 0 && rf.Log[fi-base].Term == conflictTerm; fi-- {
			}
			conflict = fi + 1
		}
		rf.mu.Unlock()
		reply.ConflictEntry = conflict
	} else {
		//not heartbeat
		newLastIndex := args.PrevLogIndex
		if len(args.Entries) > 0 {
			newLastIndex = args.Entries[len(args.Entries)-1].Index
			for _, e := range args.Entries {
				if e.Index >= base+len(rf.Log) {
					rf.Log = append(rf.Log, e)
				} else if rf.Log[e.Index-base].Term != e.Term {
					rf.Log = rf.Log[:e.Index-base]
					rf.Log = append(rf.Log, e)
				}
				change = true
			}

		}
		//check commit
		if args.LeaderCommit > rf.CommitIndex {
			if args.LeaderCommit > newLastIndex {
				if newLastIndex > rf.CommitIndex {
					rf.CommitIndex = newLastIndex
				}
			} else {
				rf.CommitIndex = args.LeaderCommit
			}
			//apply log
			go rf.applyLogEntries()
		}
		rf.mu.Unlock()
		reply.Success = true
	}
	if change {
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) checkCommit() {
	if rf.GetRole() == LEADER {
		rf.mu.Lock()
		for i := len(rf.Log) - 1; rf.Log[i].Index > rf.CommitIndex && rf.Log[i].Term == rf.CurrentTerm; i-- {
			count := 1
			for index := range rf.peers {
				if index != rf.me && rf.MatchIndex[index] >= rf.Log[i].Index {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.CommitIndex = rf.Log[i].Index
				break
			}
		}
		rf.mu.Unlock()
		// go rf.applyLogEntries()
	}
	go rf.applyLogEntries()
}

//
//index: server index
//args: args for SendAppendEtries
//reply: reply for SendAppendEntries
func (rf *Raft) processAppendEntriesReply(index int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if reply.Success && rf.Role == LEADER && args.Term == rf.CurrentTerm {
		if len(args.Entries) > 0 {

			if args.Entries[len(args.Entries)-1].Index+1 > rf.NextIndex[index] {
				rf.NextIndex[index] = args.Entries[len(args.Entries)-1].Index + 1
				rf.MatchIndex[index] = rf.NextIndex[index] - 1
			}

			if rf.MatchIndex[index] > rf.CommitIndex {
				go rf.checkCommit()
			}
		}
		rf.mu.Unlock()
	} else {
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.Role = FOLLOWER
			rf.mu.Unlock()
			go func() { rf.ToFollower <- true }()
			rf.persist()
			rf.mu.Lock()
		}
		// fmt.Println("args.Term", args.Term, "Reply.Term", reply.Term)
		if rf.Role == LEADER && args.Term == rf.CurrentTerm && args.Term >= reply.Term {
			// rf.NextIndex[index]--
			// fmt.Println("Nextindex--", rf.NextIndex)
			rf.NextIndex[index] = reply.ConflictEntry
		}
		rf.mu.Unlock()
	}
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

func (rf *Raft) makeAppendEntries(index int) AppendEntriesArgs {
	// Term         int        //leader`s term
	// LeaderId     int        //so follower can redirect clients
	// PrevLogIndex int        //index of log entry immediately preceding newe ones
	// PreLogTerm   int        //Term of prevLogIndex entry
	// Entries      []LogEntry //log entries to store(empty for heartbeat)
	// LeaderCommit int        //leader`s commitIndex
	args := AppendEntriesArgs{}

	rf.mu.Lock()
	base := rf.Log[0].Index
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.NextIndex[index] - 1
	if rf.NextIndex[index] <= base || base+len(rf.Log) < rf.NextIndex[index] {
		fmt.Println("NextIndex[index]", rf.NextIndex[index], ";len(rf.Log):", len(rf.Log))
	}
	// fmt.Println("PrevLogIndex", args.PrevLogIndex)
	args.PrevLogTerm = rf.Log[args.PrevLogIndex-base].Term
	args.LeaderCommit = rf.CommitIndex

	if rf.NextIndex[index] < base+len(rf.Log) {
		args.Entries = rf.Log[rf.NextIndex[index]-base:]
	} else {
		args.Entries = make([]LogEntry, 0)
	}
	rf.mu.Unlock()
	return args
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
	if rf.GetRole() != LEADER {
		isLeader = false
		return index, term, isLeader
	}
	//
	base := rf.Log[0].Index
	rf.mu.Lock()
	logEntry := LogEntry{command, rf.CurrentTerm, base + len(rf.Log)}
	rf.Log = append(rf.Log, logEntry)
	index = base + len(rf.Log) - 1
	term = rf.CurrentTerm
	// fmt.Println(rf.me, "Leader:", isLeader, " Start Agree:", command)
	go rf.sendHeartBeat()
	rf.mu.Unlock()
	// rf.Log[]
	rf.persist()
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
	rf.persist()
}

func (rf *Raft) candidateAction() {
	// fmt.Println(rf.me, " In Candidate")
	totalVotes := 1 // one vote for itself
	collectVote := make(chan bool)

	rf.mu.Lock()
	// fmt.Println(rf.me, "Log:", rf.Log)
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term}
	rf.mu.Unlock()
	rand.Seed(time.Now().Unix() + int64(rf.me))
	// electionTimeout := time.NewTimer(time.Duration(rand.Intn(150) + 150))
	electionTimeout := time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	for i := range rf.peers {
		if i != rf.me {
			go func(index int) {
				reply := &RequestVoteReply{}
				ok := false
				timeout := time.Now()
				//retry RPC call every 10ms, timeout is 200ms
				for rf.GetRole() == CANDIDATE && rf.CurrentTerm == args.Term && time.Since(timeout).Seconds() < 0.2 {

					var ok1 = false
					var reply1 = &RequestVoteReply{}
					t := time.NewTimer(10 * time.Microsecond)
					go func() {
						ok1 = rf.sendRequestVote(index, args, reply1)
					}()
					for j := 1; (j < 4) && (!ok1); j++ {
						<-t.C
						t.Reset(10 * time.Millisecond)
					}
					<-t.C

					if ok1 {
						ok = true
						reply.Term = reply1.Term
						reply.VoteGranted = reply1.VoteGranted
						break
					}

					// retryTime := 50 * time.Millisecond
					// checkTimeOut := time.NewTimer(time.Duration(10) * time.Millisecond)

					// go func() {
					// 	ok1 = rf.sendRequestVote(index, args, reply1)
					// }()
					// for !ok1 {
					// 	select {
					// 	case <-time.After(retryTime):
					// 		break
					// 	case <-checkTimeOut.C:
					// 		// fmt.Println("Send RequestVote to ", index)
					// 		if ok1 {
					// 			break
					// 		} else {
					// 			checkTimeOut.Reset(time.Duration(10) * time.Millisecond)
					// 		}
					// 	}
					// }
					// if ok1 {
					// 	ok = true
					// 	reply.Term = reply1.Term
					// 	reply.VoteGranted = reply1.VoteGranted
					// 	break
					// }

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
						rf.persist()
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
				close(collectVote)
				rf.becomeLeader()
				rf.sendHeartBeat()
				return
			}
		case <-electionTimeout.C:
			//restart election
			// fmt.Println(rf.me, "restart Election!")
			rf.becomeCandidate()
			return
		case <-rf.HeartBeatCh:
			// millSeconds := time.Duration(rand.Intn(150) + 150)
			// electionTimeout.Reset(millSeconds * time.Millisecond)
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
				if rf.NextIndex[index] <= rf.Log[0].Index {
					return
				}
				args := rf.makeAppendEntries(index)
				if rf.GetRole() == LEADER {
					// fmt.Println("AppendEntries:", rf.me, index, args)
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(index, args, reply)
					if ok {
						// fmt.Println(rf.me, "send heart beat to ", index, "ok", reply)
						rf.processAppendEntriesReply(index, args, reply)
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) becomeLeader() {
	// fmt.Println(rf.me, "Become leader")
	rf.mu.Lock()
	rf.Role = LEADER
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.NextIndex[i] = rf.Log[len(rf.Log)-1].Index + 1
		// rf.MatchIndex[i] = rf.Log[0].Index
		rf.MatchIndex[i] = rf.Log[0].Index
	}
	rf.mu.Unlock()
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
						if rf.NextIndex[index] <= rf.Log[0].Index {
							fmt.Println("install snapshot")
						} else {
							// timeout = 60 * time.Millisecond
							args := rf.makeAppendEntries(index)
							reply := &AppendEntriesReply{}
							if rf.GetRole() != LEADER {
								return
							}
							go func() {
								if ok := rf.sendAppendEntries(index, args, reply); ok {
									rf.processAppendEntriesReply(index, args, reply)
								}
							}()
						}
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

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.HeartBeatCh = make(chan bool)
	rf.ToFollower = make(chan bool)
	rf.ApplyCh = applyCh

	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.Role = FOLLOWER

	rf.Log = make([]LogEntry, 1)
	rf.Log[0] = LogEntry{0, 0, 0}
	data := persister.ReadRaftState()
	if len(data) != 0 {
		// fmt.Println("Recovery: ", data)
		rf.readPersist(data)
	}
	rf.LastApplied = rf.Log[0].Index
	rf.CommitIndex = rf.Log[0].Index
	// rf.readPersist(data)
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
