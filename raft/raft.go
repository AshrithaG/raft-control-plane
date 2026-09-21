// Package raft implements leader election and log replication.
//
// The implementation is deliberately passive: it owns no timers, no goroutines
// and no sockets. Time arrives through Tick and messages through Step, and both
// return the messages to send. Everything that could introduce nondeterminism
// lives outside, which is what lets the simulator in package sim replay a
// failing run exactly from its seed.
package raft

import (
	"fmt"
	"math/rand"
)

type NodeID int

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return "leader"
	}
}

// Entry is one command in the replicated log. Index is 1-based; index 0 is the
// zero entry that simplifies the log-matching comparisons.
type Entry struct {
	Term  int
	Index int
	Cmd   []byte
}

type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgApp
	MsgAppResp
)

type Message struct {
	Type    MsgType
	From    NodeID
	To      NodeID
	Term    int
	Granted bool // vote responses
	Success bool // append responses

	// Vote
	LastLogIndex int
	LastLogTerm  int

	// AppendEntries
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int

	// AppendEntries response
	MatchIndex int
	// ReadSeq carries a read barrier through a heartbeat round trip. A leader
	// that wants to serve a read locally must first hear from a quorum in its
	// own term, or it may still believe it leads after being deposed.
	ReadSeq int
	// ConflictIndex lets a leader back up by more than one entry per round
	// trip. Without it, catching up a follower that is far behind takes one
	// round trip per missing entry, which the partition tests make obvious.
	ConflictIndex int
}

// Config is per-node configuration. Timeouts are in ticks, not durations: the
// simulator decides what a tick is worth, and the real deployment can too.
// Bug names a defect to reintroduce on purpose. A checker that never fails is
// not evidence of a correct implementation until it has been shown to fail on
// implementations known to be wrong, so each of these is a real Raft mistake
// and the harness is expected to catch every one.
type Bug string

const (
	BugNone Bug = ""
	// BugCommitAnyTerm drops the rule that a leader may only commit by replica
	// count within its own term. This is the figure 8 scenario in the paper.
	BugCommitAnyTerm Bug = "commit-any-term"
	// BugNoUpToDate grants a vote to a candidate whose log is behind, which
	// lets a leader be elected that is missing committed entries.
	BugNoUpToDate Bug = "no-up-to-date"
	// BugBlindTruncate truncates at the append point instead of only where
	// entries actually conflict, so a redelivered stale AppendEntries erases
	// entries the follower already had.
	BugBlindTruncate Bug = "blind-truncate"
	// BugAcceptStaleTerm accepts AppendEntries from an older term, letting a
	// deposed leader keep writing.
	BugAcceptStaleTerm Bug = "accept-stale-term"
	// BugStaleRead serves a local read without confirming leadership with a
	// quorum first, which is the most common way a correct-looking Raft
	// implementation returns a stale value.
	BugStaleRead Bug = "stale-read"
)

type Config struct {
	ID               NodeID
	Peers            []NodeID
	ElectionTimeout  int // base; the node adds seeded jitter up to the same again
	HeartbeatTimeout int
	Rand             *rand.Rand
	Bug              Bug
}

type Node struct {
	cfg Config

	role        Role
	currentTerm int
	votedFor    NodeID // -1 for none
	log         []Entry
	commitIndex int
	lastApplied int

	nextIndex  map[NodeID]int
	matchIndex map[NodeID]int
	votes      map[NodeID]bool

	elapsed        int
	electionLimit  int
	heartbeatTimer int

	leaderID NodeID

	readSeq     int
	pendingRead map[int]*readBarrier
	readyReads  []ReadReady
}

// readBarrier is one outstanding read: the commit index observed when the read
// was requested, and the acknowledgements collected since.
type readBarrier struct {
	index int
	term  int
	acks  map[NodeID]bool
}

// ReadReady says a read barrier has been confirmed by a quorum: once the state
// machine has applied up to Index, a local read at this node is linearizable.
type ReadReady struct {
	Seq   int
	Index int
}

func New(cfg Config) *Node {
	n := &Node{
		cfg: cfg, role: Follower, votedFor: -1, leaderID: -1,
		log:        []Entry{{Term: 0, Index: 0}},
		nextIndex:   map[NodeID]int{},
		pendingRead: map[int]*readBarrier{},
		matchIndex: map[NodeID]int{},
		votes:      map[NodeID]bool{},
	}
	n.resetElectionTimer()
	return n
}

func (n *Node) ID() NodeID     { return n.cfg.ID }
func (n *Node) Role() Role     { return n.role }
func (n *Node) Term() int      { return n.currentTerm }
func (n *Node) Leader() NodeID { return n.leaderID }
func (n *Node) Commit() int    { return n.commitIndex }

// Log returns the replicated log. The simulator reads it to check log matching
// across nodes; nothing mutates it from outside.
func (n *Node) Log() []Entry { return n.log }

func (n *Node) last() Entry { return n.log[len(n.log)-1] }

func (n *Node) resetElectionTimer() {
	jitter := 0
	if n.cfg.ElectionTimeout > 0 && n.cfg.Rand != nil {
		jitter = n.cfg.Rand.Intn(n.cfg.ElectionTimeout)
	}
	n.electionLimit = n.cfg.ElectionTimeout + jitter
	n.elapsed = 0
}

// quorum is a strict majority of the whole cluster, this node included.
func (n *Node) quorum() int { return (len(n.cfg.Peers)+1)/2 + 1 }

// Tick advances this node's clock by one and returns anything it wants to send.
func (n *Node) Tick() []Message {
	var out []Message
	switch n.role {
	case Leader:
		n.heartbeatTimer++
		if n.heartbeatTimer >= n.cfg.HeartbeatTimeout {
			n.heartbeatTimer = 0
			out = append(out, n.broadcastAppend()...)
		}
	default:
		n.elapsed++
		if n.elapsed >= n.electionLimit {
			out = append(out, n.startElection()...)
		}
	}
	return out
}

func (n *Node) startElection() []Message {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.leaderID = -1
	n.resetElectionTimer()

	var out []Message
	for _, p := range n.cfg.Peers {
		out = append(out, Message{
			Type: MsgVote, From: n.cfg.ID, To: p, Term: n.currentTerm,
			LastLogIndex: n.last().Index, LastLogTerm: n.last().Term,
		})
	}
	// A single-node cluster is already a majority of itself.
	if len(n.votes) >= n.quorum() {
		out = append(out, n.becomeLeader()...)
	}
	return out
}

func (n *Node) becomeLeader() []Message {
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.heartbeatTimer = 0
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.last().Index + 1
		n.matchIndex[p] = 0
	}
	return n.broadcastAppend()
}

func (n *Node) becomeFollower(term int, leader NodeID) {
	// Outstanding reads belong to the term that opened them. Carrying them
	// across a term change would confirm a read with acknowledgements
	// collected while this node still thought it led, and carrying a confirmed
	// one would let it be served after the barrier stopped meaning anything.
	n.pendingRead = map[int]*readBarrier{}
	n.readyReads = nil
	n.role = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.leaderID = leader
	n.resetElectionTimer()
}

func (n *Node) broadcastAppend() []Message {
	var out []Message
	for _, p := range n.cfg.Peers {
		out = append(out, n.appendTo(p))
	}
	return out
}

func (n *Node) appendTo(p NodeID) Message {
	next := n.nextIndex[p]
	if next < 1 {
		next = 1
	}
	// A leader must never index past its own log, whatever a peer reports.
	// Without this clamp a follower claiming a longer match crashes the
	// leader, which turns a safety bug somewhere else into a crash here and
	// hides where the fault actually is.
	if next > n.last().Index+1 {
		next = n.last().Index + 1
	}
	prev := n.log[next-1]
	var entries []Entry
	if next <= n.last().Index {
		entries = append(entries, n.log[next:]...)
	}
	return Message{
		Type: MsgApp, From: n.cfg.ID, To: p, Term: n.currentTerm,
		PrevLogIndex: prev.Index, PrevLogTerm: prev.Term,
		Entries: entries, LeaderCommit: n.commitIndex,
	}
}

// StartRead opens a read barrier. The returned messages are heartbeats that
// carry the barrier; the read may be served once ConfirmedReads reports it and
// the state machine has applied through the recorded index.
//
// A leader that skips this and reads its local state can return a value that a
// newer leader has already overwritten, which breaks linearizability without
// breaking a single Raft invariant.
func (n *Node) StartRead() (seq int, msgs []Message, ok bool) {
	if n.role != Leader {
		return 0, nil, false
	}
	// The identifier carries the term. A counter alone restarts at 1 after a
	// crash, and a client waiting on read 1 from before the crash would be
	// served against a barrier confirmed in an earlier term: a stale read that
	// looks exactly like a consensus bug and is not one.
	n.readSeq++
	seq = n.currentTerm*1_000_000 + n.readSeq
	if n.cfg.Bug == BugStaleRead {
		// The defect: assume leadership still holds and serve immediately.
		n.readyReads = append(n.readyReads, ReadReady{Seq: seq, Index: n.commitIndex})
		return seq, nil, true
	}
	b := &readBarrier{index: n.commitIndex, term: n.currentTerm, acks: map[NodeID]bool{n.cfg.ID: true}}
	n.pendingRead[seq] = b
	if len(b.acks) >= n.quorum() {
		n.readyReads = append(n.readyReads, ReadReady{Seq: seq, Index: b.index})
		delete(n.pendingRead, seq)
		return seq, nil, true
	}
	for _, p := range n.cfg.Peers {
		m := n.appendTo(p)
		m.ReadSeq = seq
		msgs = append(msgs, m)
	}
	return seq, msgs, true
}

// ConfirmedReads returns read barriers confirmed since the last call.
func (n *Node) ConfirmedReads() []ReadReady {
	out := n.readyReads
	n.readyReads = nil
	return out
}

// Applied reports how far the caller's state machine may safely read.
func (n *Node) Applied() int { return n.lastApplied }

// Propose appends a command if this node is the leader. The caller learns the
// index to watch; it is committed only when Commit passes it.
func (n *Node) Propose(cmd []byte) (index int, ok bool) {
	if n.role != Leader {
		return 0, false
	}
	e := Entry{Term: n.currentTerm, Index: n.last().Index + 1, Cmd: cmd}
	n.log = append(n.log, e)
	n.matchIndex[n.cfg.ID] = e.Index
	return e.Index, true
}

// PendingAppends returns fresh AppendEntries for every peer, used after a
// proposal so a command does not wait for the next heartbeat.
func (n *Node) PendingAppends() []Message {
	if n.role != Leader {
		return nil
	}
	return n.broadcastAppend()
}

// Step handles one delivered message and returns the replies to send.
func (n *Node) Step(m Message) []Message {
	// Any message from a newer term makes this node a follower before the
	// message is handled on its merits.
	if m.Term > n.currentTerm {
		leader := NodeID(-1)
		if m.Type == MsgApp {
			leader = m.From
		}
		n.becomeFollower(m.Term, leader)
	}

	switch m.Type {
	case MsgVote:
		return n.handleVote(m)
	case MsgVoteResp:
		return n.handleVoteResp(m)
	case MsgApp:
		return n.handleApp(m)
	case MsgAppResp:
		return n.handleAppResp(m)
	}
	return nil
}

func (n *Node) handleVote(m Message) []Message {
	grant := false
	if m.Term >= n.currentTerm && (n.votedFor == -1 || n.votedFor == m.From) {
		// Raft's up-to-date rule: a candidate whose log is behind must not win,
		// or a committed entry could be lost.
		last := n.last()
		upToDate := m.LastLogTerm > last.Term ||
			(m.LastLogTerm == last.Term && m.LastLogIndex >= last.Index)
		if n.cfg.Bug == BugNoUpToDate {
			upToDate = true
		}
		if upToDate {
			grant = true
			n.votedFor = m.From
			n.resetElectionTimer()
		}
	}
	return []Message{{Type: MsgVoteResp, From: n.cfg.ID, To: m.From, Term: n.currentTerm, Granted: grant}}
}

func (n *Node) handleVoteResp(m Message) []Message {
	if n.role != Candidate || m.Term != n.currentTerm || !m.Granted {
		return nil
	}
	n.votes[m.From] = true
	if len(n.votes) >= n.quorum() {
		return n.becomeLeader()
	}
	return nil
}

func (n *Node) handleApp(m Message) []Message {
	reply := Message{Type: MsgAppResp, From: n.cfg.ID, To: m.From, Term: n.currentTerm}
	if m.Term < n.currentTerm && n.cfg.Bug != BugAcceptStaleTerm {
		reply.Success = false
		return []Message{reply}
	}
	n.role = Follower
	n.leaderID = m.From
	n.resetElectionTimer()

	if m.PrevLogIndex > n.last().Index {
		reply.Success = false
		reply.ConflictIndex = n.last().Index + 1
		return []Message{reply}
	}
	if n.log[m.PrevLogIndex].Term != m.PrevLogTerm {
		bad := n.log[m.PrevLogIndex].Term
		i := m.PrevLogIndex
		for i > 0 && n.log[i].Term == bad {
			i--
		}
		reply.Success = false
		reply.ConflictIndex = i + 1
		return []Message{reply}
	}

	// Append, truncating only where the incoming entries actually conflict. A
	// blind truncate here would discard committed entries when a stale but
	// valid AppendEntries is redelivered.
	if n.cfg.Bug == BugBlindTruncate && len(m.Entries) > 0 {
		n.log = n.log[:m.PrevLogIndex+1]
		n.log = append(n.log, m.Entries...)
	} else {
		for _, e := range m.Entries {
			if e.Index <= n.last().Index {
				if n.log[e.Index].Term == e.Term {
					continue
				}
				n.log = n.log[:e.Index]
			}
			n.log = append(n.log, e)
		}
	}
	if m.LeaderCommit > n.commitIndex {
		n.commitIndex = min(m.LeaderCommit, n.last().Index)
	}
	reply.Success = true
	reply.MatchIndex = n.last().Index
	reply.ReadSeq = m.ReadSeq // echo the barrier back to the leader
	return []Message{reply}
}

func (n *Node) handleAppResp(m Message) []Message {
	if n.role != Leader || m.Term != n.currentTerm {
		return nil
	}
	if !m.Success {
		next := m.ConflictIndex
		if next < 1 {
			next = 1
		}
		n.nextIndex[m.From] = next
		return []Message{n.appendTo(m.From)}
	}
	if m.ReadSeq > 0 {
		if b, ok := n.pendingRead[m.ReadSeq]; ok && b.term == n.currentTerm {
			b.acks[m.From] = true
			if len(b.acks) >= n.quorum() {
				n.readyReads = append(n.readyReads, ReadReady{Seq: m.ReadSeq, Index: b.index})
				delete(n.pendingRead, m.ReadSeq)
			}
		}
	}
	match := m.MatchIndex
	if match > n.last().Index {
		match = n.last().Index // a peer cannot have matched what this leader does not have
	}
	n.matchIndex[m.From] = match
	n.nextIndex[m.From] = match + 1
	n.advanceCommit()
	return nil
}

// advanceCommit moves the commit index to the highest entry replicated on a
// majority, and only for entries from the current term. Committing an older
// term's entry by counting replicas is the classic Raft bug (figure 8).
func (n *Node) advanceCommit() {
	for idx := n.last().Index; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm && n.cfg.Bug != BugCommitAnyTerm {
			continue
		}
		count := 1 // itself
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			return
		}
	}
}

// Apply returns newly committed entries in order, exactly once.
func (n *Node) Apply() []Entry {
	var out []Entry
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		out = append(out, n.log[n.lastApplied])
	}
	return out
}

func (n *Node) String() string {
	return fmt.Sprintf("n%d(%s,t%d,c%d,len%d)", n.cfg.ID, n.role, n.currentTerm, n.commitIndex, n.last().Index)
}

// Persistent is the state Raft requires to survive a crash: current term, the
// vote, and the log. Everything else is volatile and must be rebuilt, which is
// what makes a restart a useful fault to inject rather than a no-op.
type Persistent struct {
	Term     int
	VotedFor NodeID
	Log      []Entry
}

func (n *Node) Persistent() Persistent {
	log := make([]Entry, len(n.log))
	copy(log, n.log)
	return Persistent{Term: n.currentTerm, VotedFor: n.votedFor, Log: log}
}

// Restore rebuilds a node after a crash from its persisted state alone. It
// comes back as a follower with no commit index, exactly as a real restart
// would, so a bug that relies on volatile state surviving shows up here.
func Restore(cfg Config, p Persistent) *Node {
	n := New(cfg)
	n.currentTerm = p.Term
	n.votedFor = p.VotedFor
	n.log = make([]Entry, len(p.Log))
	copy(n.log, p.Log)
	return n
}
