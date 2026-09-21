package raft

import (
	"math/rand"
	"testing"
)

func cluster(n int, bug Bug) map[NodeID]*Node {
	nodes := map[NodeID]*Node{}
	for i := 0; i < n; i++ {
		var peers []NodeID
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, NodeID(j))
			}
		}
		nodes[NodeID(i)] = New(Config{
			ID: NodeID(i), Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 3,
			Rand: rand.New(rand.NewSource(int64(i) + 1)), Bug: bug,
		})
	}
	return nodes
}

// deliver runs messages to completion with no faults, which is all these unit
// tests need; the fault cases live in the simulator.
func deliver(nodes map[NodeID]*Node, msgs []Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		msgs = msgs[1:]
		msgs = append(msgs, nodes[m.To].Step(m)...)
	}
}

func elect(t *testing.T, nodes map[NodeID]*Node, id NodeID) {
	t.Helper()
	n := nodes[id]
	for i := 0; i < 40 && n.Role() != Leader; i++ {
		deliver(nodes, n.Tick())
	}
	if n.Role() != Leader {
		t.Fatalf("n%d never became leader", id)
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	nodes := cluster(1, BugNone)
	elect(t, nodes, 0)
}

func TestLeaderReplicatesAndCommits(t *testing.T) {
	nodes := cluster(3, BugNone)
	elect(t, nodes, 0)
	leader := nodes[0]
	idx, ok := leader.Propose([]byte("set x 1"))
	if !ok {
		t.Fatal("leader refused a proposal")
	}
	deliver(nodes, leader.PendingAppends())
	deliver(nodes, leader.PendingAppends()) // second round carries the commit index
	if leader.Commit() < idx {
		t.Fatalf("commit index %d never reached %d", leader.Commit(), idx)
	}
	for id, n := range nodes {
		log := n.Log()
		if len(log) <= idx || string(log[idx].Cmd) != "set x 1" {
			t.Fatalf("n%d missing the entry at index %d", id, idx)
		}
	}
}

// A candidate whose log is behind must not collect votes, because a leader
// missing a committed entry can erase it.
func TestStaleCandidateLosesTheVote(t *testing.T) {
	nodes := cluster(3, BugNone)
	elect(t, nodes, 0)
	nodes[0].Propose([]byte("committed"))
	deliver(nodes, nodes[0].PendingAppends())

	stale := nodes[2]
	stale.log = stale.log[:1] // roll this node's log back behind the others
	msgs := stale.startElection()
	granted := 0
	for _, m := range msgs {
		for _, r := range nodes[m.To].Step(m) {
			if r.Type == MsgVoteResp && r.Granted {
				granted++
			}
		}
	}
	if granted > 0 {
		t.Fatalf("a candidate with a truncated log collected %d votes", granted)
	}
}

// A read barrier must not be confirmed by acknowledgements alone: it needs a
// quorum, and it must not survive a term change.
func TestReadBarrierNeedsAQuorum(t *testing.T) {
	nodes := cluster(3, BugNone)
	elect(t, nodes, 0)
	leader := nodes[0]
	seq, msgs, ok := leader.StartRead()
	if !ok {
		t.Fatal("leader refused to open a read barrier")
	}
	if got := leader.ConfirmedReads(); len(got) != 0 {
		t.Fatalf("read %d confirmed before any peer answered", seq)
	}
	// One peer answers: still short of a quorum of three.
	deliver(nodes, []Message{msgs[0]})
	one := leader.ConfirmedReads()
	deliver(nodes, []Message{msgs[1]})
	two := leader.ConfirmedReads()
	if len(one)+len(two) == 0 {
		t.Fatal("read never confirmed even after both peers answered")
	}
}

func TestStaleReadBugConfirmsWithoutAnyone(t *testing.T) {
	nodes := cluster(3, BugStaleRead)
	elect(t, nodes, 0)
	if _, _, ok := nodes[0].StartRead(); !ok {
		t.Fatal("read refused")
	}
	if len(nodes[0].ConfirmedReads()) != 1 {
		t.Fatal("the stale-read defect should confirm immediately, and the harness relies on it doing so")
	}
}

// A leader must not commit an older term's entry by counting replicas, which
// is the figure 8 case from the Raft paper.
func TestCommitRequiresAnEntryFromTheCurrentTerm(t *testing.T) {
	nodes := cluster(3, BugNone)
	elect(t, nodes, 0)
	leader := nodes[0]
	leader.log = append(leader.log, Entry{Term: leader.Term() - 1, Index: 1, Cmd: []byte("old")})
	for _, p := range leader.cfg.Peers {
		leader.matchIndex[p] = 1
	}
	leader.advanceCommit()
	if leader.Commit() != 0 {
		t.Fatalf("committed an entry from an earlier term by replica count (commit=%d)", leader.Commit())
	}
}
