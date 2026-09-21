package lincheck

import (
	"testing"

	"github.com/AshrithaG/raft-control-plane/kv"
)

func put(client, seq int, key, val string, call, ret int) Op {
	return Op{Client: client, Call: call, Return: ret, Result: "ok",
		Cmd: kv.Command{Client: client, Seq: seq, Kind: kv.Put, Key: key, Val: val}}
}

func get(client, seq int, key, want string, call, ret int) Op {
	return Op{Client: client, Call: call, Return: ret, Result: want,
		Cmd: kv.Command{Client: client, Seq: seq, Kind: kv.Get, Key: key}}
}

// The checker is only worth running once it agrees with histories whose answer
// is known by hand, so these come in pairs: one legal, one not.
func TestSequentialHistoryIsLinearizable(t *testing.T) {
	h := []Op{put(0, 1, "a", "1", 0, 1), get(0, 2, "a", "1", 2, 3)}
	if r := Check(h); !r.OK {
		t.Fatalf("a plainly sequential history was rejected: %s", r.Reason)
	}
}

func TestReadThatMissesACompletedWriteIsNot(t *testing.T) {
	h := []Op{put(0, 1, "a", "1", 0, 1), get(1, 1, "a", "", 2, 3)}
	if r := Check(h); r.OK {
		t.Fatal("accepted a read that missed a write which had already returned")
	}
}

// A read overlapping a write may legally see either outcome.
func TestConcurrentReadMaySeeEitherValue(t *testing.T) {
	before := []Op{put(0, 1, "a", "1", 0, 10), get(1, 1, "a", "", 1, 2)}
	after := []Op{put(0, 1, "a", "1", 0, 10), get(1, 1, "a", "1", 1, 2)}
	if r := Check(before); !r.OK {
		t.Fatalf("read before the concurrent write took effect was rejected: %s", r.Reason)
	}
	if r := Check(after); !r.OK {
		t.Fatalf("read after the concurrent write took effect was rejected: %s", r.Reason)
	}
}

// An operation whose result the client never learned may have taken effect or
// not, and both readings have to be accepted.
func TestPendingOperationMayOrMayNotHaveHappened(t *testing.T) {
	pending := put(0, 1, "a", "1", 0, 5)
	pending.Pending = true
	dropped := []Op{pending, get(1, 1, "a", "", 6, 7)}
	applied := []Op{pending, get(1, 1, "a", "1", 6, 7)}
	if r := Check(dropped); !r.OK {
		t.Fatalf("rejected a history where the timed-out write never happened: %s", r.Reason)
	}
	if r := Check(applied); !r.OK {
		t.Fatalf("rejected a history where the timed-out write did happen: %s", r.Reason)
	}
}

// Two clients cannot both see their own value as the final one.
func TestContradictoryReadsAreRejected(t *testing.T) {
	h := []Op{
		put(0, 1, "a", "x", 0, 1),
		put(1, 1, "a", "y", 2, 3),
		get(0, 2, "a", "x", 4, 5),
	}
	if r := Check(h); r.OK {
		t.Fatal("accepted a read of an overwritten value after the newer write returned")
	}
}

// CAS against a value written by someone else must fail, and the checker has
// to know that too.
func TestCASAgainstStaleValue(t *testing.T) {
	cas := Op{Client: 1, Call: 4, Return: 5, Result: "false",
		Cmd: kv.Command{Client: 1, Seq: 1, Kind: kv.CAS, Key: "a", Old: "x", Val: "z"}}
	h := []Op{put(0, 1, "a", "x", 0, 1), put(0, 2, "a", "y", 2, 3), cas}
	if r := Check(h); !r.OK {
		t.Fatalf("a CAS that correctly failed was rejected: %s", r.Reason)
	}
	cas.Result = "true"
	h[2] = cas
	if r := Check(h); r.OK {
		t.Fatal("accepted a CAS that claimed to succeed against a stale value")
	}
}
