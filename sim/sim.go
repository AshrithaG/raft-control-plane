// Package sim runs a Raft cluster inside a deterministic virtual world.
//
// Nothing here uses real time, real sockets or goroutines. One seed fixes every
// delay, every drop, every partition and every crash, so a run that fails can
// be replayed exactly from that seed alone. That property is the point of the
// package: a consensus bug that only appears in one interleaving out of
// thousands is useless if it cannot be reproduced.
package sim

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sort"

	"github.com/AshrithaG/raft-control-plane/raft"
)

type event struct {
	at  int // virtual tick of delivery
	seq int // tie-break, so equal ticks still have one fixed order
	msg raft.Message
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)        { *q = append(*q, x.(event)) }
func (q *eventQueue) Pop() (x any)      { old := *q; x = old[len(old)-1]; *q = old[:len(old)-1]; return }

// Faults describes what the world does to messages and nodes.
type Faults struct {
	MinDelay  int     // ticks
	MaxDelay  int     // ticks
	DropRate  float64 // fraction of messages lost outright
	DupRate   float64 // fraction delivered twice
	Partition float64 // chance per tick of starting a partition
	PartLen   int     // ticks a partition lasts
	CrashRate float64 // chance per tick of crashing a live node
	CrashLen  int     // ticks a crashed node stays down
}

// Violation is an invariant that failed, with the tick it failed at so the run
// can be replayed to just before it.
type Violation struct {
	Tick int
	Rule string
	Det  string
}

func (v Violation) String() string { return fmt.Sprintf("tick %d: %s: %s", v.Tick, v.Rule, v.Det) }

type Sim struct {
	Nodes  map[raft.NodeID]*raft.Node
	ids    []raft.NodeID
	rng    *rand.Rand
	q      eventQueue
	seq    int
	tick   int
	faults Faults

	down       map[raft.NodeID]int // node -> tick it comes back
	partUntil  int
	partGroups [][]raft.NodeID

	// leaderByTerm records the leader elected in each term, so a second leader
	// in the same term is caught the moment it appears.
	leaderByTerm map[int]raft.NodeID
	// committed records what each index committed to, cluster-wide, so a node
	// that applies something different is caught.
	committed  map[int]string
	Violations []Violation
	Applied    map[raft.NodeID][]raft.Entry
	bug        raft.Bug
	// reads records confirmed read barriers per node: sequence to the commit
	// index the read may be served at.
	reads map[raft.NodeID]map[int]int
}

// NewWithBug builds a cluster whose nodes all carry a deliberate defect, used
// to show the harness catches implementations that are known to be wrong.
func NewWithBug(n int, seed int64, f Faults, bug raft.Bug) *Sim {
	s := New(n, seed, f)
	for _, id := range s.ids {
		var peers []raft.NodeID
		for _, other := range s.ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		s.Nodes[id] = raft.New(raft.Config{
			ID: id, Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 3,
			Rand: rand.New(rand.NewSource(seed*1000 + int64(id))), Bug: bug,
		})
	}
	s.bug = bug
	return s
}

func New(n int, seed int64, f Faults) *Sim {
	rng := rand.New(rand.NewSource(seed))
	s := &Sim{
		Nodes: map[raft.NodeID]*raft.Node{}, rng: rng, faults: f,
		down: map[raft.NodeID]int{}, leaderByTerm: map[int]raft.NodeID{},
		committed: map[int]string{}, Applied: map[raft.NodeID][]raft.Entry{},
		reads: map[raft.NodeID]map[int]int{},
	}
	for i := 0; i < n; i++ {
		s.ids = append(s.ids, raft.NodeID(i))
	}
	for _, id := range s.ids {
		var peers []raft.NodeID
		for _, other := range s.ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		s.Nodes[id] = raft.New(raft.Config{
			ID: id, Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 3,
			// Each node draws from its own stream so that adding a node does
			// not reshuffle the others' timeouts.
			Rand: rand.New(rand.NewSource(seed*1000 + int64(id))),
		})
	}
	heap.Init(&s.q)
	return s
}

func (s *Sim) Tick() int { return s.tick }

func (s *Sim) isDown(id raft.NodeID) bool { return s.down[id] > s.tick }

// reachable reports whether a message can cross between two nodes right now.
func (s *Sim) reachable(a, b raft.NodeID) bool {
	if s.isDown(a) || s.isDown(b) {
		return false
	}
	if s.tick >= s.partUntil || len(s.partGroups) == 0 {
		return true
	}
	group := func(id raft.NodeID) int {
		for gi, g := range s.partGroups {
			for _, m := range g {
				if m == id {
					return gi
				}
			}
		}
		return -1
	}
	return group(a) == group(b)
}

func (s *Sim) send(msgs []raft.Message) {
	for _, m := range msgs {
		if !s.reachable(m.From, m.To) || s.rng.Float64() < s.faults.DropRate {
			continue
		}
		delay := s.faults.MinDelay
		if s.faults.MaxDelay > s.faults.MinDelay {
			delay += s.rng.Intn(s.faults.MaxDelay - s.faults.MinDelay + 1)
		}
		s.seq++
		heap.Push(&s.q, event{at: s.tick + delay, seq: s.seq, msg: m})
		if s.rng.Float64() < s.faults.DupRate {
			s.seq++
			heap.Push(&s.q, event{at: s.tick + delay + 1, seq: s.seq, msg: m})
		}
	}
}

// Step advances the world one tick: injects faults, ticks live nodes, delivers
// everything due, applies committed entries and checks the invariants.
func (s *Sim) Step() {
	s.tick++
	s.injectFaults()

	for _, id := range s.ids {
		if s.isDown(id) {
			continue
		}
		s.send(s.Nodes[id].Tick())
	}
	for s.q.Len() > 0 && s.q[0].at <= s.tick {
		ev := heap.Pop(&s.q).(event)
		if s.isDown(ev.msg.To) || !s.reachable(ev.msg.From, ev.msg.To) {
			continue
		}
		s.send(s.Nodes[ev.msg.To].Step(ev.msg))
	}
	for _, id := range s.ids {
		if s.isDown(id) {
			continue
		}
		for _, r := range s.Nodes[id].ConfirmedReads() {
			if s.reads[id] == nil {
				s.reads[id] = map[int]int{}
			}
			s.reads[id][r.Seq] = r.Index
		}
	}
	s.applyAll()
	s.check()
}

func (s *Sim) injectFaults() {
	if s.faults.Partition > 0 && s.tick >= s.partUntil && s.rng.Float64() < s.faults.Partition {
		perm := s.rng.Perm(len(s.ids))
		cut := 1 + s.rng.Intn(len(s.ids)-1)
		var a, b []raft.NodeID
		for i, p := range perm {
			if i < cut {
				a = append(a, s.ids[p])
			} else {
				b = append(b, s.ids[p])
			}
		}
		s.partGroups = [][]raft.NodeID{a, b}
		s.partUntil = s.tick + s.faults.PartLen
	}
	if s.faults.CrashRate > 0 && s.rng.Float64() < s.faults.CrashRate {
		id := s.ids[s.rng.Intn(len(s.ids))]
		if !s.isDown(id) {
			// A crash keeps only what Raft says is persistent; everything else
			// is rebuilt on restart.
			p := s.Nodes[id].Persistent()
			var peers []raft.NodeID
			for _, other := range s.ids {
				if other != id {
					peers = append(peers, other)
				}
			}
			s.Nodes[id] = raft.Restore(raft.Config{
				ID: id, Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 3,
				Rand: rand.New(rand.NewSource(int64(s.tick)*31 + int64(id))),
				Bug:  s.bug,
			}, p)
			// Confirmed read barriers do not survive the crash: the state that
			// justified them is gone with the volatile state.
			delete(s.reads, id)
			s.down[id] = s.tick + s.faults.CrashLen
		}
	}
}

func (s *Sim) applyAll() {
	for _, id := range s.ids {
		if s.isDown(id) {
			continue
		}
		for _, e := range s.Nodes[id].Apply() {
			s.Applied[id] = append(s.Applied[id], e)
			if prev, ok := s.committed[e.Index]; ok && prev != string(e.Cmd) {
				s.Violations = append(s.Violations, Violation{
					Tick: s.tick, Rule: "state machine safety",
					Det: fmt.Sprintf("index %d applied as %q by n%d after %q elsewhere", e.Index, e.Cmd, id, prev),
				})
			} else if !ok {
				s.committed[e.Index] = string(e.Cmd)
			}
		}
	}
}

// check runs the invariants that must hold after every tick.
func (s *Sim) check() {
	// Election safety: one leader per term.
	for _, id := range s.ids {
		n := s.Nodes[id]
		if s.isDown(id) || n.Role() != raft.Leader {
			continue
		}
		if prev, ok := s.leaderByTerm[n.Term()]; ok && prev != id {
			s.Violations = append(s.Violations, Violation{
				Tick: s.tick, Rule: "election safety",
				Det: fmt.Sprintf("term %d has leaders n%d and n%d", n.Term(), prev, id),
			})
		}
		s.leaderByTerm[n.Term()] = id
	}

	// Log matching: two logs agreeing at an index and term agree on everything
	// before it, which this checks the cheap way, entry by entry.
	for i := 0; i < len(s.ids); i++ {
		for j := i + 1; j < len(s.ids); j++ {
			a, b := s.Nodes[s.ids[i]].Log(), s.Nodes[s.ids[j]].Log()
			n := len(a)
			if len(b) < n {
				n = len(b)
			}
			for k := 0; k < n; k++ {
				if a[k].Term == b[k].Term && string(a[k].Cmd) != string(b[k].Cmd) {
					s.Violations = append(s.Violations, Violation{
						Tick: s.tick, Rule: "log matching",
						Det: fmt.Sprintf("index %d term %d differs: n%d %q vs n%d %q",
							k, a[k].Term, s.ids[i], a[k].Cmd, s.ids[j], b[k].Cmd),
					})
				}
			}
		}
	}
}

// Leader returns the current leader if exactly one live node claims it.
func (s *Sim) Leader() (raft.NodeID, bool) {
	var found raft.NodeID = -1
	for _, id := range s.ids {
		if s.isDown(id) {
			continue
		}
		if s.Nodes[id].Role() == raft.Leader {
			if found != -1 {
				return -1, false
			}
			found = id
		}
	}
	return found, found != -1
}

// Propose sends a command to the current leader, returning the index to watch.
func (s *Sim) Propose(cmd []byte) (raft.NodeID, int, bool) {
	id, ok := s.Leader()
	if !ok {
		return -1, 0, false
	}
	idx, ok := s.Nodes[id].Propose(cmd)
	if !ok {
		return -1, 0, false
	}
	s.send(s.Nodes[id].PendingAppends())
	return id, idx, true
}

// CommittedAt reports whether an index is committed on the given node with the
// command the caller proposed, which is what a client needs before it can
// report success.
func (s *Sim) CommittedAt(id raft.NodeID, idx int, cmd []byte) bool {
	n := s.Nodes[id]
	if s.isDown(id) || n.Commit() < idx {
		return false
	}
	log := n.Log()
	return idx < len(log) && string(log[idx].Cmd) == string(cmd)
}

// Summary is what a run reports when it finishes.
func (s *Sim) Summary() string {
	terms := make([]int, 0, len(s.leaderByTerm))
	for t := range s.leaderByTerm {
		terms = append(terms, t)
	}
	sort.Ints(terms)
	maxCommit := 0
	for _, id := range s.ids {
		if c := s.Nodes[id].Commit(); c > maxCommit {
			maxCommit = c
		}
	}
	return fmt.Sprintf("ticks=%d elections=%d maxCommit=%d violations=%d",
		s.tick, len(terms), maxCommit, len(s.Violations))
}

// CommittedPrefix returns the agreed log prefix: entries 1..N where N is the
// highest commit index any live node has reached. Committed entries never
// change, so reading them from whichever node is furthest ahead is safe.
func (s *Sim) CommittedPrefix() []raft.Entry {
	best := raft.NodeID(-1)
	high := 0
	for _, id := range s.ids {
		if s.isDown(id) {
			continue
		}
		if c := s.Nodes[id].Commit(); c > high {
			high, best = c, id
		}
	}
	if best == -1 || high == 0 {
		return nil
	}
	log := s.Nodes[best].Log()
	if high >= len(log) {
		high = len(log) - 1
	}
	out := make([]raft.Entry, 0, high)
	out = append(out, log[1:high+1]...)
	return out
}

// Down reports whether a node is currently crashed, which a driver needs so it
// does not wait forever on a node that cannot answer.
func (s *Sim) Down(id raft.NodeID) bool { return s.isDown(id) }

// StartRead opens a read barrier on the current leader. The read may be served
// once ReadReady reports an index and the caller's state machine has applied
// that far.
func (s *Sim) StartRead() (raft.NodeID, int, bool) {
	id, ok := s.Leader()
	if !ok {
		return -1, 0, false
	}
	seq, msgs, ok := s.Nodes[id].StartRead()
	if !ok {
		return -1, 0, false
	}
	s.send(msgs)
	return id, seq, true
}

// ReadReady reports the index a confirmed read may be served at.
func (s *Sim) ReadReady(id raft.NodeID, seq int) (int, bool) {
	m, ok := s.reads[id]
	if !ok {
		return 0, false
	}
	idx, ok := m[seq]
	return idx, ok
}

// AppliedThrough reports how far a node's state machine may be read.
func (s *Sim) AppliedThrough(id raft.NodeID) int { return s.Nodes[id].Applied() }
