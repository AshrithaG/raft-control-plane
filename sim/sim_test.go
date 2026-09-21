package sim

import (
	"fmt"
	"testing"
)

func fingerprint(s *Sim) string {
	out := s.Summary()
	for id := 0; id < len(s.ids); id++ {
		n := s.Nodes[s.ids[id]]
		out += fmt.Sprintf("|n%d t%d c%d", id, n.Term(), n.Commit())
		for _, e := range n.Log() {
			out += fmt.Sprintf(",%d:%d", e.Index, e.Term)
		}
	}
	return out
}

func drive(seed int64, f Faults) string {
	s := New(5, seed, f)
	for t := 0; t < 1500; t++ {
		if t%7 == 0 {
			s.Propose([]byte(fmt.Sprintf("cmd-%d", t)))
		}
		s.Step()
	}
	return fingerprint(s)
}

// The README's central claim: a seed reproduces a run exactly. If a map
// iteration or a wall clock ever leaks into the simulator, two runs of the same
// seed diverge and this fails, long before anyone tries to replay a real bug.
func TestSameSeedSameRun(t *testing.T) {
	chaos := Faults{MinDelay: 1, MaxDelay: 6, DropRate: 0.08, DupRate: 0.05,
		Partition: 0.01, PartLen: 40, CrashRate: 0.008, CrashLen: 25}
	for seed := int64(1); seed <= 5; seed++ {
		a, b := drive(seed, chaos), drive(seed, chaos)
		if a != b {
			t.Fatalf("seed %d produced two different runs", seed)
		}
	}
}

// And a different seed has to produce a different run, or the seed is not
// actually driving anything.
func TestDifferentSeedDifferentRun(t *testing.T) {
	chaos := Faults{MinDelay: 1, MaxDelay: 6, DropRate: 0.08, Partition: 0.01, PartLen: 40}
	if drive(1, chaos) == drive(2, chaos) {
		t.Fatal("seeds 1 and 2 produced identical runs")
	}
}

func TestBaselineHoldsInvariantsUnderChaos(t *testing.T) {
	chaos := Faults{MinDelay: 1, MaxDelay: 6, DropRate: 0.08, DupRate: 0.05,
		Partition: 0.01, PartLen: 40, CrashRate: 0.008, CrashLen: 25}
	for seed := int64(1); seed <= 10; seed++ {
		s := New(5, seed, chaos)
		for t := 0; t < 1500; t++ {
			if t%7 == 0 {
				s.Propose([]byte(fmt.Sprintf("cmd-%d", t)))
			}
			s.Step()
		}
		if len(s.Violations) > 0 {
			t.Fatalf("seed %d: %s", seed, s.Violations[0])
		}
	}
}
