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

import "sync"
import "labrpc"

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"
)

// interface{}: empty interface, interface{} 类型是没有方法的接口,所有类型都实现了空接口
//              这里我认为是command可标识为read/write，值与寄存器的类型也可变，所以不用具体类型表示，(或者可以用string做字符串解析？)
/*	names := []string{"stanley", "david", "oscar"}
    vals := make([]interface{}, len(names))
    for i, v := range names {
        vals[i] = v
    }*/
// 函数名前的括号: 接收者,加*可修改接收器,不加则修改无效
/*	func (m *Mutatable) Mutate() {
    	m.a = 5
    	m.b = 7
	}
    m := &Mutatable{0, 0} m.Mutate()
*/

const FOLLOWER_STATE int = 0
const CANDIDATE_STATE int = 1
const LEADER_STATE int = 2
const HeartbeatPeriod = 70.0 * time.Millisecond
const ElectionTimeBase = 300.0 * time.Millisecond

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

type logEntry struct{
	Index 	int
	Term 	int
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//
/*type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}*/
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state  	int   // the state of server (follower/candidate/leader)

	// persistent state on all servers , Updated on stable storage before responding to RPCs
	currentTerm		int   // latest term server has seen (initialized to -1)
	votedFor		int	  // candidateId that received vote in current term(initialized to -1)
	log				[]logEntry

	// volatile state on all servers
	commitIndex		int   // index of highest log entry known to be committed (initialized to -1)
	lastApplied		int   // index of highest log entry applied to state machine (initialized to -1)

	// volatile state on leaders , reinitialized after election
	nextIndex		[]int // index of the next log entry to send to some server (initialized to leader last log index+1)
	matchIndex		[]int // index of highest log entry known to be replicated on server

	// timers of heartbeat and election timeout
	heartbeatTimeOut	int
	electionTimeOut		int
	timer 				*time.Timer

	applyChannel chan ApplyMsg //a channel on which the tester or service expects Raft to send ApplyMsg messages.
	alive	     chan int      // if alive then 1, killed than 0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// because there exists lock in RequestVote, there is no need to lock
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.electionTimeOut)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
// 恢复机制,很显然需要和persist()有相同的存储和读取方式保证对应数据一致
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.electionTimeOut)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int  // candidate's term
	CandidateId 	int	 // candidate requesting vote
	LastLogIndex	int  // index of candidate's last log entry
	LastLogTerm		int	 // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term			int  // currentTerm, for candidate to update itself
	VoteGranted		bool // true means candidate received vote
}

type AppendEntriesArgs struct {

}

type AppendEntriesReply struct {

}

func (rf *Raft) ResetTimer(){
	rf.mu.Lock()
	fmt.Println("Server ",rf.me,": Reset timer.")
	defer rf.mu.Unlock()
	rf.timer.Reset(time.Duration(rf.electionTimeOut)*time.Millisecond)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//var readyVote bool = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm{
		// change state to become a follower
		rf.state = FOLLOWER_STATE
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = args.Term
	} else{
		reply.Term = rf.currentTerm
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
		}
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

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *RequestVoteReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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


	return rf
}
