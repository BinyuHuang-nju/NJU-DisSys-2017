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

/*
第一次测试: 卡在TestReElection，错误原因：server2 断连后，server0与server1循环选举，并判断到对方的term更大而转为follower，从此往复
	找到原因：在StartElection中，if reply.Term > rf.currentTerm 写成了 rf.me
*/

const FOLLOWER_STATE int = 0
const CANDIDATE_STATE int = 1
const LEADER_STATE int = 2
const HeartbeatPeriod = 100
const ElectionTimeBase = 300

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
	heartbeatTimer	*time.Timer

	// timers of heartbeat and election timeout
	heartbeatTimeOut	int
	electionTimeOut		int
	electionTimer 		*time.Timer

	applyChannel chan ApplyMsg //a channel on which the tester or service expects Raft to send ApplyMsg messages.
	killed	     chan int      // if alive then 0, killed than 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == LEADER_STATE
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
	// But I change the location to move persist() out of mu.lock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

type RequestVoteArgs struct {
	// Your data here.
	Term			int  // candidate's term
	CandidateId 	int	 // candidate requesting vote
	LastLogIndex	int  // index of candidate's last log entry
	LastLogTerm		int	 // term of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here.
	Term			int  // currentTerm, for candidate to update itself
	VoteGranted		bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term			int  // leader's term
	LeaderId		int	 // so follower can redirect clients
	PrevLogIndex	int	 // index of log entry immediately preceding new ones
	PrevLogTerm		int  // term of prevLogIndex entry
	Entries 		[]logEntry  // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int  // leader's commitIndex
}

type AppendEntriesReply struct {
	Term			int  // currentTerm, for leader to update itself
	Success			bool // false if term stale
	Contain			bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	NextTestIndex	int
}

func Min(x, y int) int {
	if x<y {
		return x
	}
	return y
}

func (rf *Raft) ResetElectionTimer(){
	//rf.mu.Lock() // one operation is atomic and does not need to add lock
	//defer rf.mu.Unlock()
	rf.electionTimer.Reset(time.Duration(rf.electionTimeOut)*time.Millisecond)
}
func (rf *Raft) ResetHeartbeatTimer(){
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.heartbeatTimer.Reset(time.Duration(rf.heartbeatTimeOut)*time.Millisecond)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
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
		reply.VoteGranted = true
	} else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		if (rf.votedFor != -1 && rf.votedFor != args.CandidateId) || rf.state == LEADER_STATE {
			reply.VoteGranted = false
		}
	}
	if reply.VoteGranted == true {
		// the log of candidate should be more up-to-date
		var idx int = len(rf.log)-1
		if idx >= 0 {
			if args.LastLogTerm > rf.log[idx].Term ||
				(args.LastLogTerm == rf.log[idx].Term && args.LastLogIndex >= rf.log[idx].Index) {
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		}
	}
	if reply.VoteGranted == true {
		rf.votedFor = args.CandidateId
		rf.ResetElectionTimer() // new term, reset time of election
	}
	fmt.Println("======================================================")
	fmt.Println("Server ",rf.me,": Receive RequestVote from server ",args.CandidateId)
	fmt.Println("before this, votedFor:",rf.votedFor," ,currentTerm:",rf.currentTerm," so result:",reply.VoteGranted)
	fmt.Println("======================================================")
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	reply.Success = true
	reply.Term = args.Term
	rf.ResetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER_STATE
		rf.currentTerm = args.Term
	} else {
		if rf.state == LEADER_STATE {
			fmt.Println("It is impossible!!! There are two leaders in the same term! ")
		} else if rf.state == CANDIDATE_STATE {
			rf.state = FOLLOWER_STATE
		}
	}
	if args.PrevLogIndex < 0 {
		reply.Contain = true
		reply.NextTestIndex = -1
	} else if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Contain = false
		var targetIndex int = Min(args.PrevLogIndex-1,len(rf.log)-1)
		for targetIndex >= 0 && rf.log[targetIndex].Term > args.PrevLogTerm {
			targetIndex--
		}
		reply.NextTestIndex = targetIndex
	} else {
		reply.Contain = true
		reply.NextTestIndex = args.PrevLogIndex + 1
	}
	if len(args.Entries) == 0 {  // heartbeat
		// have reset election timer
	} else {                    // normal AppendEntries RPC

	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
	}
	if rf.commitIndex > rf.lastApplied {
		for i:= rf.lastApplied+1; i<= rf.commitIndex; i++ {
			var msg ApplyMsg
			msg.Index = i
			msg.Command = rf.log[i].Command
			rf.applyChannel <- msg
		}
		rf.lastApplied = rf.commitIndex
	}
	fmt.Println("======================================================")
	fmt.Println("Server ",rf.me,": Receive AppendEntries from server(leader) ",args.LeaderId)
	if len(args.Entries) == 0 {
		fmt.Println("The AppendEntries RPC is heartbeat")
	}
	fmt.Println("======================================================")
	rf.mu.Unlock()
	rf.persist()
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

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool{
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
	rf.mu.Lock()
	index := -1
	term := -1
	isLeader := true

	rf.mu.Unlock()
	rf.persist()	// because there exists lock in persist(), so it should not be lock()-> lock()-> unlock()-> unlock()
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
	rf.killed <- 1
}

func (rf *Raft)StartElection() {
	// times out, starts election
	rf.mu.Lock()
	rf.state = CANDIDATE_STATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.ResetElectionTimer()
	var lli, llt, le int = -1, -1, len(rf.log)
	if le > 0 {
		lli = rf.log[le-1].Index
		llt = rf.log[le-1].Term
	}
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = lli
	args.LastLogTerm = llt
	rf.mu.Unlock()

	replyCh := make(chan *RequestVoteReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// there is no lock, so the server may receive RequestVote or AppendEntries anytime and may turn to follower
		if rf.state == FOLLOWER_STATE {
			break
		}
		go rf.ReadyToSendRequestVote(i, args, replyCh)
	}
	if rf.state == FOLLOWER_STATE {
		fmt.Println("1")
		rf.ResetElectionTimer()
		return // if not func, change to 'break'
	}
	//理论上在timeout后这里应该在下一个candidate里处理，这里保留后续可做优化
	time.Sleep(time.Duration(rf.electionTimeOut-rf.heartbeatTimeOut) * time.Millisecond)
	close(replyCh)

	rf.mu.Lock()
	var voteNum, total int = 1, len(rf.peers)
	for reply := range replyCh {
		if reply.Term > rf.currentTerm { // because the decision if rf.state == FOLLOWER_STATE before, here rf.currentTerm == args.Term
			rf.state = FOLLOWER_STATE
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		} else {
			if reply.VoteGranted == true {
				voteNum = voteNum + 1
			}
		}
	}
	fmt.Println("======================================================")
	fmt.Println("Server ",rf.me," state:",rf.state,", voteNum:",voteNum,", total:",total)
	if rf.state == FOLLOWER_STATE {
		rf.ResetElectionTimer()
		fmt.Println("Turn to follower.")
		fmt.Println("======================================================")
	} else if voteNum >= total/2+1 {
		rf.state = LEADER_STATE
		// rf.BecomeLeader()
	} else { // times out, new election
		rf.state = CANDIDATE_STATE
		rf.electionTimeOut = rand.Int()%ElectionTimeBase + ElectionTimeBase
		rf.ResetElectionTimer()
		fmt.Println("Server ",rf.me," times out, starts new election, new electionThreld:",rf.electionTimeOut)
		fmt.Println("======================================================")
	}
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft)ReadyToSendRequestVote(target int, args RequestVoteArgs, replyCh chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	timeout := time.NewTimer(time.Duration(rf.electionTimeOut - rf.heartbeatTimeOut)*time.Millisecond)
	var received bool = false
	for received == false {
		select {
		case <-timeout.C:
			return
		default:
			received = rf.sendRequestVote(target, args, reply)
		}
	}
	if received == true {
		replyCh <- reply
	}
	fmt.Println("======================================================")
	fmt.Println("Server ",rf.me,": Send RequestVote to server ",target, " currentTerm:",args.Term )
	if received == true {
		fmt.Println("The RequestVote RPC has been sent successfully.")
	}
	fmt.Println("======================================================")
}

func (rf *Raft)ReadyToSendAppendEntries(target int, args AppendEntriesArgs, replyCh chan *AppendEntriesReply) {
	reply := &AppendEntriesReply{}
	var received bool = false
	received = rf.sendAppendEntries(target, args, reply)
	if received == true {
		replyCh <- reply
	}
	fmt.Println("======================================================")
	fmt.Println("Server ",rf.me,": Send AppendEntries to server ",target)
	if len(args.Entries) ==0 {
		fmt.Println("This is a heartbeat.")
	}
	if received == true {
		fmt.Println("The AppendEntries RPC has been sent successfully.")
	}
	fmt.Println("======================================================")
}

func (rf *Raft)BecomeLeader() {
	rf.mu.Lock()
	rf.state = LEADER_STATE
	rf.votedFor = rf.me
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
	rf.ResetElectionTimer()
	fmt.Println("======================================================")
	fmt.Println("Server ",rf.me," has become a leader.")
	fmt.Println("======================================================")
	rf.mu.Unlock()
}

func (rf *Raft)LeaderSendHeartbeat() {
	rf.mu.Lock()
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = -1
	args.PrevLogTerm = -1
	if len(rf.log) > 0 {
		args.PrevLogIndex = rf.log[len(rf.log)-1].Index
		args.PrevLogTerm = rf.log[len(rf.log)-1].Term
	}
	args.Entries = []logEntry{}
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	replyCh := make(chan *AppendEntriesReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// there is no lock, so the server may receive RequestVote or AppendEntries anytime and may turn to follower
		if rf.state == FOLLOWER_STATE {
			break
		}
		go rf.ReadyToSendAppendEntries(i, args, replyCh)
	}
	if rf.state == FOLLOWER_STATE {
		rf.ResetElectionTimer()
		return // if not func, change to 'break'
	}

	// there should be code to handle replys from followers
	rf.mu.Lock()
	rf.ResetHeartbeatTimer()

	rf.mu.Unlock()
}

func (rf *Raft) Loop() {
	rf.ResetElectionTimer()
	rf.ResetHeartbeatTimer()
	for {
		select {
		case <-rf.electionTimer.C:
			rf.ResetElectionTimer()
			var ifLeader_NewLeader bool = false
			if rf.state == FOLLOWER_STATE {
				// times out, starts election
				ifLeader_NewLeader = true
				rf.StartElection()
			}else if rf.state == CANDIDATE_STATE { // there should not be 'else if',because rf may times out and start new election
				ifLeader_NewLeader = true
				rf.StartElection()
			}
			if ifLeader_NewLeader == true && rf.state == LEADER_STATE {
				rf.BecomeLeader()
				rf.LeaderSendHeartbeat()
			}
		case <-rf.heartbeatTimer.C:
			switch rf.state {
			case FOLLOWER_STATE:
				rf.ResetHeartbeatTimer()
			case CANDIDATE_STATE:
				rf.ResetHeartbeatTimer()
			case LEADER_STATE:
				rf.LeaderSendHeartbeat()
			}
		case <-rf.killed:
			fmt.Println("Server ",rf.me," has crashed.")
			return
		default:

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
	// step1. initialize each variables of server node
	// step2. create a go routine to judge whether start election
	// step3. if step2 passes, then become a leader and send heartbeat and AppendEntriesRPC
	// step4. if crash, end; if lose leadership, goto 2
	// Your initialization code here.
	rf.state = FOLLOWER_STATE

	// initialize from state persisted before a crash
	if persister.ReadRaftState() != nil {
		rf.readPersist(persister.ReadRaftState())
	} else {
		rf.currentTerm = -1
		rf.votedFor = -1
		rf.log = []logEntry{}
		rf.electionTimeOut = rand.Int()%ElectionTimeBase + ElectionTimeBase
	}
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.heartbeatTimeOut = HeartbeatPeriod
	rf.electionTimer = time.NewTimer(time.Duration(rf.electionTimeOut)*time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(rf.heartbeatTimeOut)*time.Millisecond)
	rf.applyChannel = applyCh
	rf.killed = make(chan int)

	go rf.Loop()

	return rf
}
