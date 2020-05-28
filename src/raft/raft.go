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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

const voteForNone = -1
const heartbeatTime = 200
const electionTimeout = 600
const checkInterval = 10
const follower = "Follorer"
const leader = "Leader"
const candidate = "Candidate"

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term  int
	Index int
	Logs  interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh                chan ApplyMsg
	lastRPCTime            time.Time
	CurrentTerm            int
	VoteFor                int
	Logs                   []LogEntry
	commitIndex            int
	LastApplied            int
	Role                   string
	nextIndex              []int
	matchIndex             []int
	receivedVoteNum        int
	receivedGrantedVoteNum int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) initServerIndex() {
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.Logs))
		rf.matchIndex = append(rf.matchIndex, 0)
	}
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.CurrentTerm
	isleader = rf.Role == leader
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//Your code here (2C).
	//fmt.Println("persist()")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	e.Encode(rf.Role)
	e.Encode(rf.LastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VoteFor int
	var Logs []LogEntry
	var Role string
	var LastApplied int
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VoteFor) != nil ||
		d.Decode(&Logs) != nil ||
		d.Decode(&Role) != nil ||
		d.Decode(&LastApplied) != nil {
		fmt.Println("readPersist fail")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VoteFor = VoteFor
		rf.Logs = Logs
		rf.Role = Role
		//rf.LastApplied = LastApplied  comment to avoid unexpected bug
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int

	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool

	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrecLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	ConflictIndex int
	ConflictTerm  int
	Term          int
	Success       bool
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.Logs[len(rf.Logs)-1]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//DPrintf("Server %d recieve RequestVote from server %d, %v\n", rf.me, args.CandidateID, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	rf.CurrentTerm = args.Term
	rf.VoteFor = voteForNone
	rf.Role = follower
	lastLog := rf.getLastLog()
	reply.Term = rf.CurrentTerm
	//DPrintf("last log is %v\n", lastLog)
	if args.LastLogTerm < lastLog.Term {
		reply.VoteGranted = false
	} else {
		if args.LastLogTerm > lastLog.Term || (args.LastLogIndex >= lastLog.Index && args.LastLogTerm == lastLog.Term) {
			reply.VoteGranted = true
			rf.VoteFor = args.CandidateID
			rf.lastRPCTime = time.Now()
		} else {
			reply.VoteGranted = false
		}
	}
	rf.persist()
	//DPrintf("Server %d reply is %v\n", rf.me, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("Server %d receive appendEntries from %d, args=%v\n", rf.me, args.LeaderID, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	rf.lastRPCTime = time.Now()
	rf.CurrentTerm = args.Term
	rf.Role = follower
	reply.Term = rf.CurrentTerm
	isContainLastLog := false
	var i int
	for i = len(rf.Logs) - 1; i >= 0; i-- {
		if rf.Logs[i].Term == args.PrecLogTerm && rf.Logs[i].Index == args.PrevLogIndex {
			reply.ConflictIndex = -1
			reply.ConflictTerm = -1
			isContainLastLog = true
			break
		}
		if rf.Logs[i].Index == args.PrevLogIndex && rf.Logs[i].Term != args.PrecLogTerm {
			reply.ConflictTerm = rf.Logs[i].Term
			isContainLastLog = false
			for i > 0 {
				if rf.Logs[i].Term == reply.ConflictTerm && rf.Logs[i-1].Term != reply.ConflictTerm {
					reply.ConflictIndex = rf.Logs[i].Index
					break
				}
				i--
			}
			break
		}
		if rf.Logs[i].Index < args.PrevLogIndex {
			isContainLastLog = false
			reply.ConflictTerm = -1
			reply.ConflictIndex = len(rf.Logs)
			break
		}

	}
	//fmt.Printf("success=%v, PrecLogTerm=%d, PrevLogIndex=%d, log=%d, nextIndex=%v, from %d to %d\n", isContainLastLog, args.PrecLogTerm, args.PrevLogIndex, rf.log, rf.nextIndex, args.LeaderID, rf.me)
	reply.Success = isContainLastLog
	if !isContainLastLog {
		return
	}
	if len(args.Entries) > 0 {
		i = i + 1
		for j, logEntry := range args.Entries {
			if i <= len(rf.Logs)-1 && rf.Logs[i].Term == logEntry.Term {
				i++
			} else {
				rf.Logs = append(rf.Logs[:i], args.Entries[j:]...)
				break
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		if len(args.Entries) > 0 {
			rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
		} else {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
		}

	}
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.Role == leader
	if isLeader {
		rf.mu.Lock()
		log := LogEntry{
			Term:  rf.CurrentTerm,
			Index: rf.getLastLog().Index + 1,
			Logs:  command,
		}
		DPrintf("Leader %d receive cmd = %v\n", rf.me, log.Logs)
		rf.Logs = append(rf.Logs, log)
		rf.persist()
		rf.matchIndex[rf.me] = log.Index
		rf.nextIndex[rf.me] = log.Index + 1
		index = log.Index
		term = log.Term
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.getLastLog().Index >= rf.nextIndex[i] {
				go func(server int) {
					for {
						if rf.Role != leader {
							return
						}
						rf.mu.Lock()
						var i int
						for i = len(rf.Logs) - 1; i > 1; i-- {
							if rf.Logs[i].Index == rf.nextIndex[server] {
								break
							}
						}
						args := AppendEntriesArgs{
							Term:         rf.CurrentTerm,
							LeaderID:     rf.me,
							LeaderCommit: rf.commitIndex,
							PrevLogIndex: rf.Logs[i-1].Index,
							PrecLogTerm:  rf.Logs[i-1].Term,
							Entries:      rf.Logs[i:],
						}
						rf.mu.Unlock()
						reply := AppendEntriesReply{}
						ok := rf.sendAppendEntries(server, &args, &reply)
						if rf.Role != leader {
							return
						}
						if ok {
							if reply.Success {
								rf.mu.Lock()
								rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
								rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
								rf.mu.Unlock()
								return
							}
							rf.mu.Lock()
							if reply.Term > rf.CurrentTerm {
								rf.Role = follower
								rf.CurrentTerm = reply.Term
								rf.mu.Unlock()
								return
							}

							rf.nextIndex[server] = rf.optimizedBacktracking(reply)
							rf.mu.Unlock()
						} else {
							time.Sleep(10 * time.Millisecond)
						}
					}
				}(i)
			}
		}
	}
	// Your code here (2B).
	return index, term, isLeader
}

func (rf *Raft) optimizedBacktracking(reply AppendEntriesReply) int {
	// if reply.ConflictIndex == 0 {
	// 	return 1 //Hardcode to avoid out of range bug
	// }
	if reply.ConflictTerm == -1 {
		return reply.ConflictIndex
	}
	for i := len(rf.Logs) - 1; i > 0; i-- {
		if rf.Logs[i].Term == reply.ConflictTerm && rf.Logs[i+1].Term != reply.ConflictTerm {
			return rf.Logs[i].Index
		}
	}
	return reply.ConflictIndex
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionCheck() {
	for {
		randomTimeout := time.Duration(rand.Int()%electionTimeout+electionTimeout) * time.Millisecond
		time.Sleep(20 * time.Millisecond)
		if time.Now().Sub(rf.lastRPCTime) > randomTimeout {
			go rf.election()
		}
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.Role = candidate
	rf.VoteFor = rf.me
	rf.CurrentTerm++
	rf.lastRPCTime = time.Now()
	rf.receivedVoteNum = 1
	rf.receivedGrantedVoteNum = 1
	rf.persist()
	rf.mu.Unlock()
	DPrintf("Server %d start election, term = %d.\n", rf.me, rf.CurrentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				args := RequestVoteArgs{}
				args.CandidateID = rf.me
				args.Term = rf.CurrentTerm
				lastLog := rf.getLastLog()
				args.LastLogIndex = lastLog.Index
				args.LastLogTerm = lastLog.Term
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					//DPrintf("sendRequestVote from %d to %d failed.\n", rf.me, server)
					rf.mu.Lock()
					rf.receivedVoteNum++
					if reply.VoteGranted {
						rf.receivedGrantedVoteNum++
					}
					if reply.Term > rf.CurrentTerm {
						DPrintf("Server %d go back to follower in election cause server %d has higher term.\n", rf.me, server)
						rf.CurrentTerm = reply.Term
						rf.Role = follower
						rf.VoteFor = voteForNone
						rf.persist()
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
	for {
		time.Sleep(checkInterval * time.Millisecond)
		if time.Now().Sub(rf.lastRPCTime) > time.Duration(electionTimeout)*time.Millisecond || rf.Role != candidate {
			break
		}
		if rf.receivedGrantedVoteNum > len(rf.peers)/2 {
			DPrintf("Server %d win the election and become the leader.\n", rf.me)
			rf.mu.Lock()
			rf.Role = leader
			rf.initServerIndex()
			go rf.leaderCheckCommitedIndex()
			rf.mu.Unlock()
			for j := range rf.peers {
				if j != rf.me {
					go rf.heartbeat(j)
				}
			}
			break
		}
	}
}

func (rf *Raft) heartbeat(server int) {
	for {
		if rf.Role != leader {
			//DPrintf("Server %d is not leader\n", rf.me)
			return
		}
		rf.mu.Lock()
		rf.lastRPCTime = time.Now()
		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		args.Term = rf.CurrentTerm
		args.LeaderID = rf.me
		args.Entries = []LogEntry{}
		prevLog := rf.Logs[rf.nextIndex[server]-1]
		args.PrecLogTerm = prevLog.Term
		args.PrevLogIndex = prevLog.Index
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()
		//DPrintf("Server %d send heartbeat to %d\n", rf.me, server)
		ok := rf.sendAppendEntries(server, &args, &reply)
		if ok {
			if reply.Success {

			} else {
				if reply.Term > args.Term {
					DPrintf("Server %d go back to follower cause heartbeat from server %d.\n", rf.me, server)
					rf.mu.Lock()
					rf.VoteFor = voteForNone
					rf.Role = follower
					rf.persist()
					rf.mu.Unlock()
					return
				}
				rf.mu.Lock()
				rf.nextIndex[server] = rf.optimizedBacktracking(reply)
				rf.mu.Unlock()
			}

		}
		time.Sleep(heartbeatTime * time.Millisecond)
	}
}

func (rf *Raft) applyLog() {
	for {
		if rf.commitIndex > rf.LastApplied {
			rf.mu.Lock()
			for i := rf.LastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.Logs[i].Logs,
					CommandIndex: i,
				}
				DPrintf("Server %d apply log index = %d, cmd = %v\n", rf.me, i, applyMsg.Command)
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.LastApplied = i
				rf.mu.Lock()
				//DPrintf("-------------\n")
			}

			rf.mu.Unlock()
		}
		time.Sleep(checkInterval * time.Millisecond)
	}
}

func (rf *Raft) leaderCheckCommitedIndex() {
	for {
		time.Sleep(checkInterval * time.Millisecond)
		if rf.Role != leader {
			return
		}
		rf.mu.Lock()
		matchIndex := make([]int, len(rf.matchIndex))
		copy(matchIndex, rf.matchIndex)
		sort.Ints(matchIndex)
		midIndex := matchIndex[len(matchIndex)/2]
		if rf.commitIndex < midIndex && rf.Logs[midIndex].Term == rf.CurrentTerm {
			//DPrintf("Leader %d update commitIndex to %d\n", rf.me, midIndex)
			rf.commitIndex = midIndex
		}
		rf.mu.Unlock()
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
	rf.applyCh = applyCh
	rf.me = me
	rf.dead = 0
	rf.VoteFor = voteForNone
	rf.lastRPCTime = time.Now()
	rf.commitIndex = 0
	rf.LastApplied = 0
	rf.receivedVoteNum = 0
	rf.receivedGrantedVoteNum = 0
	rf.Role = follower
	rf.Logs = []LogEntry{{Term: 0, Index: 0}}
	rf.initServerIndex()
	// Your initialization code here (2A, 2B, 2C).
	go rf.electionCheck()
	go rf.applyLog()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
