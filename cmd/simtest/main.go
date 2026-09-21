// Command simtest runs the cluster under fault schedules and checks two things
// after every run: the Raft invariants, and whether the clients could have been
// served by a single sequential store.
//
// Each run is identified by its seed and fault profile. A failure prints both,
// and rerunning with -seed reproduces it exactly.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"

	"github.com/AshrithaG/raft-control-plane/kv"
	"github.com/AshrithaG/raft-control-plane/lincheck"
	"github.com/AshrithaG/raft-control-plane/raft"
	"github.com/AshrithaG/raft-control-plane/sim"
)

type profile struct {
	name   string
	faults sim.Faults
}

func profiles() []profile {
	return []profile{
		{"quiet", sim.Faults{MinDelay: 1, MaxDelay: 3}},
		{"lossy", sim.Faults{MinDelay: 1, MaxDelay: 5, DropRate: 0.1, DupRate: 0.05}},
		{"partitions", sim.Faults{MinDelay: 1, MaxDelay: 4, DropRate: 0.02, Partition: 0.01, PartLen: 40}},
		{"crashes", sim.Faults{MinDelay: 1, MaxDelay: 4, DropRate: 0.02, CrashRate: 0.01, CrashLen: 30}},
		{"chaos", sim.Faults{MinDelay: 1, MaxDelay: 6, DropRate: 0.08, DupRate: 0.05,
			Partition: 0.01, PartLen: 40, CrashRate: 0.008, CrashLen: 25}},
		// mayhem exists because the milder profiles did not catch three of the
		// four deliberate defects: long partitions are what let a deposed
		// leader keep writing and a stale log win an election.
		{"mayhem", sim.Faults{MinDelay: 1, MaxDelay: 8, DropRate: 0.12, DupRate: 0.08,
			Partition: 0.04, PartLen: 90, CrashRate: 0.02, CrashLen: 40}},
	}
}

// client issues one operation at a time and waits for it to commit.
type client struct {
	id      int
	seq     int
	pending *lincheck.Op
	node    raft.NodeID
	index   int
	waited  int
	// readSeq is set when the outstanding operation is a local read behind a
	// read barrier rather than a command going through the log.
	readSeq int
	readKey string
}

type runResult struct {
	seed       int64
	profile    string
	summary    string
	violations []sim.Violation
	lin        lincheck.Result
	history    []lincheck.Op
	ops        int
}

// run drives one seeded run to completion. A panic inside the implementation
// under test is a detection like any other: it is recorded against the seed
// that produced it rather than taking the whole harness down, which matters
// because a crash is exactly what several real consensus bugs look like.
func run(seed int64, p profile, nodes, ticks, clients, maxOps int, bug raft.Bug) (res runResult) {
	defer func() {
		if r := recover(); r != nil {
			res.seed, res.profile = seed, p.name
			res.summary = fmt.Sprintf("panic after %d ops", res.ops)
			res.violations = append(res.violations, sim.Violation{
				Rule: "panic", Det: fmt.Sprint(r),
			})
			res.lin = lincheck.Result{OK: true, Reason: "not checked: the run panicked"}
		}
	}()
	s := sim.NewWithBug(nodes, seed, p.faults, bug)
	rng := rand.New(rand.NewSource(seed ^ 0x5eed))
	keys := []string{"a", "b"}

	cs := make([]*client, clients)
	for i := range cs {
		cs[i] = &client{id: i}
	}
	// Every node runs its own state machine over its own committed log, and a
	// client reads the result from the node it actually talked to. Deriving
	// results from one canonical store instead would make the history
	// self-consistent by construction, and the linearizability checker could
	// never fail no matter how badly the cluster diverged.
	models := map[raft.NodeID]*kv.State{}
	applied := map[raft.NodeID]int{}
	results := map[raft.NodeID]map[int]string{}
	for id := range s.Nodes {
		models[id] = kv.NewState()
		results[id] = map[int]string{}
	}
	var history []lincheck.Op

	for t := 0; t < ticks; t++ {
		s.Step()

		for id, node := range s.Nodes {
			if s.Down(id) {
				continue
			}
			log := node.Log()
			for idx := applied[id] + 1; idx <= node.Commit() && idx < len(log); idx++ {
				if c, ok := kv.Decode(log[idx].Cmd); ok {
					results[id][idx] = models[id].Apply(c)
				}
				applied[id] = idx
			}
		}

		for _, c := range cs {
			if c.pending != nil {
				c.waited++
				if c.readSeq > 0 {
					// A read is served from the contacted node's own state
					// machine, once its barrier is confirmed and it has applied
					// that far. This is the path where a stale read shows up.
					if idx, ok := s.ReadReady(c.node, c.readSeq); ok && s.AppliedThrough(c.node) >= idx && !s.Down(c.node) {
						c.pending.Result = models[c.node].Get(c.readKey)
						c.pending.Return = s.Tick()
						history = append(history, *c.pending)
						c.pending, c.readSeq = nil, 0
						continue
					}
					if c.waited > 120 {
						c.pending.Pending = true
						c.pending.Return = s.Tick()
						history = append(history, *c.pending)
						c.pending, c.readSeq = nil, 0
					}
					continue
				}
				if res, ok := results[c.node][c.index]; ok && s.CommittedAt(c.node, c.index, c.pending.Cmd.Encode()) {
					c.pending.Result = res
					c.pending.Return = s.Tick()
					history = append(history, *c.pending)
					c.pending = nil
					continue
				}
				// A command that has not committed in time is left pending:
				// the client does not know whether it took effect, and the
				// checker has to allow either.
				if c.waited > 120 {
					c.pending.Pending = true
					c.pending.Return = s.Tick()
					history = append(history, *c.pending)
					c.pending = nil
				}
				continue
			}
			if len(history)+countPending(cs) >= maxOps {
				continue
			}
			c.seq++
			key := keys[rng.Intn(len(keys))]
			cmd := kv.Command{Client: c.id, Seq: c.seq, Kind: kv.Put, Key: key,
				Val: fmt.Sprintf("c%d-%d", c.id, c.seq)}
			switch rng.Intn(3) {
			case 1:
				cmd.Kind = kv.Get
				cmd.Val = ""
			case 2:
				cmd.Kind = kv.CAS
				cmd.Old = fmt.Sprintf("c%d-%d", c.id, c.seq-1)
			}
			if cmd.Kind == kv.Get {
				node, seq, ok := s.StartRead()
				if !ok {
					c.seq--
					continue
				}
				c.node, c.readSeq, c.readKey, c.waited = node, seq, key, 0
				c.pending = &lincheck.Op{Client: c.id, Cmd: cmd, Call: s.Tick()}
				continue
			}
			node, idx, ok := s.Propose(cmd.Encode())
			if !ok {
				c.seq-- // no leader right now; try again next tick
				continue
			}
			c.node, c.index, c.waited = node, idx, 0
			c.pending = &lincheck.Op{Client: c.id, Cmd: cmd, Call: s.Tick()}
		}
	}
	for _, c := range cs {
		if c.pending != nil {
			c.pending.Pending = true
			c.pending.Return = s.Tick()
			history = append(history, *c.pending)
		}
	}

	res = runResult{seed: seed, profile: p.name, summary: s.Summary(),
		violations: s.Violations, history: history, ops: len(history)}
	if len(history) > 0 && len(history) <= 64 {
		res.lin = lincheck.Check(history)
	} else {
		res.lin = lincheck.Result{OK: true, Reason: "history outside the checker window"}
	}
	return res
}

func countPending(cs []*client) int {
	n := 0
	for _, c := range cs {
		if c.pending != nil {
			n++
		}
	}
	return n
}

func main() {
	seeds := flag.Int("seeds", 20, "seeds per fault profile")
	nodes := flag.Int("nodes", 5, "cluster size")
	ticks := flag.Int("ticks", 1500, "virtual ticks per run")
	clients := flag.Int("clients", 3, "concurrent clients")
	maxOps := flag.Int("max-ops", 40, "operations recorded per run, bounded by the checker window")
	only := flag.String("profile", "", "run one profile by name")
	seed := flag.Int64("seed", -1, "replay a single seed")
	bug := flag.String("bug", "", "run with a deliberate defect: commit-any-term, no-up-to-date, blind-truncate, accept-stale-term")
	mutations := flag.Bool("mutations", false, "check that every deliberate defect is caught")
	flag.Parse()

	if *mutations {
		runMutations(*seeds, *nodes, *ticks, *clients, *maxOps)
		return
	}

	failures := 0
	total := 0
	fmt.Printf("%-12s %6s  %-46s %5s %s\n", "profile", "seed", "summary", "ops", "linearizable")
	for _, p := range profiles() {
		if *only != "" && *only != p.name {
			continue
		}
		for i := 0; i < *seeds; i++ {
			sd := int64(i + 1)
			if *seed >= 0 {
				sd = *seed
			}
			r := run(sd, p, *nodes, *ticks, *clients, *maxOps, raft.Bug(*bug))
			total++
			ok := len(r.violations) == 0 && r.lin.OK
			if !ok {
				failures++
			}
			mark := "yes"
			if !r.lin.OK {
				mark = "NO"
			}
			fmt.Printf("%-12s %6d  %-46s %5d %s\n", p.name, sd, r.summary, r.ops, mark)
			for _, v := range r.violations {
				fmt.Printf("    VIOLATION %s\n", v)
			}
			if !r.lin.OK {
				fmt.Printf("    %s\n", r.lin.Reason)
				if min := lincheck.MinimalPrefix(r.history); len(min) > 0 {
					fmt.Printf("    shortest failing prefix, %d of %d operations:\n", len(min), len(r.history))
					fmt.Print(indent(lincheck.Describe(min)))
				} else {
					fmt.Print(indent(lincheck.Describe(r.history)))
				}
			}
			if *seed >= 0 {
				break
			}
		}
	}
	fmt.Printf("\n%d runs, %d failed\n", total, failures)
	if failures > 0 {
		os.Exit(1)
	}
}

func indent(s string) string {
	out := ""
	for _, line := range splitLines(s) {
		if line != "" {
			out += "      " + line + "\n"
		}
	}
	return out
}

func splitLines(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == '\n' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// runMutations reintroduces each known defect and reports the first seed and
// profile that catches it. A defect nothing catches is a gap in the harness,
// not a harmless bug, and is printed as such.
func runMutations(seeds, nodes, ticks, clients, maxOps int) {
	bugs := []raft.Bug{raft.BugCommitAnyTerm, raft.BugNoUpToDate, raft.BugBlindTruncate,
		raft.BugAcceptStaleTerm, raft.BugStaleRead}
	fmt.Printf("%-18s %-12s %6s  %s\n", "defect", "caught by", "seed", "how")
	missed := 0
	for _, b := range bugs {
		found := false
		for _, p := range profiles() {
			for i := 0; i < seeds && !found; i++ {
				r := run(int64(i+1), p, nodes, ticks, clients, maxOps, b)
				how := ""
				if len(r.violations) > 0 {
					how = r.violations[0].Rule
				} else if !r.lin.OK {
					how = "linearizability"
				}
				if how != "" {
					fmt.Printf("%-18s %-12s %6d  %s\n", b, p.name, i+1, how)
					found = true
				}
			}
			if found {
				break
			}
		}
		if !found {
			missed++
			fmt.Printf("%-18s %-12s %6s  NOT CAUGHT in %d seeds per profile\n", b, "-", "-", seeds)
		}
	}
	fmt.Printf("\n%d of %d defects caught\n", len(bugs)-missed, len(bugs))
	if missed > 0 {
		os.Exit(1)
	}
}
