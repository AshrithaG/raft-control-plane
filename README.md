# raft-control-plane

Raft in Go, with the part that is actually hard: a deterministic simulator that
replays any failure from its seed, and a linearizability checker that judges
what clients could legally have seen.

An implementation that passes its own unit tests proves very little about
consensus. What the tests below do is run the cluster under partitions, crashes,
message loss, duplication and reordering, then ask two separate questions after
every run: did Raft's own invariants hold, and could a single sequential store
have produced this client history?

## How determinism works

`raft.Node` owns no timers, goroutines or sockets. Time arrives through `Tick`,
messages arrive through `Step`, and both return the messages to send. Everything
nondeterministic lives in `sim`: message delay, drops, duplicates, partitions
and crashes all come from one seeded stream, and events at equal virtual ticks
are ordered by a sequence number rather than by map iteration.

The consequence is the property the project exists for: a run that fails prints
its seed, and rerunning that seed reproduces the failure exactly.

```bash
go run ./cmd/simtest -profile mayhem -seed 201 -ticks 2500
```

## Reads go through a barrier

Reads are the interesting path. A leader that answers from local state can
return a value a newer leader has already overwritten, and that breaks no Raft
invariant: the logs agree, one leader per term, nothing to see. It only shows up
against a linearizability checker.

So reads use a read barrier: the leader records its commit index, confirms with
a quorum that it still leads in the current term, and serves only once its state
machine has applied through that index. `raft.BugStaleRead` removes the
confirmation, and the checker catches it.

## Does the harness actually catch anything?

A checker that never fails is not evidence until it has failed on
implementations known to be wrong. Five real Raft defects are wired behind
`-bug`, each switched on in the implementation itself, and the matrix reports
the first seed and fault profile that catches each.

| defect | what it removes | caught by | how |
|---|---|---|---|
| `no-up-to-date` | the up-to-date check on votes, so a behind candidate can win | partitions, seed 1 | state machine safety |
| `blind-truncate` | conflict-only truncation, so a stale AppendEntries erases entries | mayhem, seed 13 | state machine safety |
| `accept-stale-term` | the term check on AppendEntries, so a deposed leader keeps writing | chaos, seed 37 | linearizability |
| `stale-read` | the read barrier, so a leader answers from local state | partitions, seed 32 | linearizability |
| `commit-any-term` | the current-term rule on commit, which is figure 8 in the paper | mayhem, seed 387, 3000 ticks | state machine safety |

Baseline for the same matrix: 150 runs across six fault profiles, no invariant
violations and every history linearizable.

Two of the five are caught only by the linearizability checker and not by any
invariant, which is the argument for having both.

Figure 8 is the outlier and the numbers should be read carefully. It needs a
specific sequence of leader changes with specific log states, so random fault
injection finds it rarely: one detection in 400 seeded runs under `mayhem`, at
3000 ticks rather than the default 2500, and not at all in 400 runs each of
`chaos` and `partitions`. It is a true positive, not a flake: the same seed on
the correct implementation is clean, while the defective one produces 20 state
machine safety violations starting at tick 1004.

An earlier run appeared to catch it at seed 201. That was not a detection: the
same seed failed on the correct implementation too, for the read-barrier reason
described below. A scripted figure 8 scenario would catch it reliably and is
worth building; a seed count is a poor substitute.

```bash
go test ./...                                  # unit tests, including checker self-tests
go run ./cmd/simtest -seeds 25 -ticks 2500     # baseline: every profile, no defects
go run ./cmd/simtest -mutations -seeds 40      # every defect must be caught
```

## Two bugs the harness found in this repo

Both were found by running it, not by reading the code.

**A stale read that the read barrier was supposed to prevent.** Seed 201 under
`mayhem`: a read at tick 1158 returned a value that two writes, both of which
had returned by tick 1007, had overwritten. The cause was not in Raft's log
handling. Read barriers were identified by a counter that restarts at 1 after a
crash, so a client still waiting on barrier 1 from before the crash was matched
to a barrier confirmed in an earlier term. Barrier identifiers now carry the
term, a restart discards confirmed barriers, and a term change clears them.

**A checker that convicted a correct implementation.** Before that, the same
seed failed for a different reason: the checker forced every timed-out operation
into the order. An operation whose result the client never learned may never
have taken effect, and a history where it did not is equally legal. The search
now branches on both readings. This is why `lincheck` has its own tests against
six histories whose answers are known by hand: a checker nobody has checked is
not a checker.

## What it does not do

- No log compaction or snapshots, so a long run keeps the whole log in memory.
- No membership change. The cluster size is fixed at startup.
- Histories are checked in a 64-operation window, because the search memoizes on
  a 64-bit set of linearized operations. Longer runs record more operations than
  they check.
- Byzantine faults are out of scope: nodes crash, messages are lost, delayed,
  duplicated and reordered, but nobody lies.
- The virtual clock has no notion of real time, so nothing here says anything
  about throughput or latency on a real network.

## Layout

| path | what is in it |
|---|---|
| `raft/` | elections, replication, read barriers, and the five deliberate defects |
| `sim/` | the deterministic world: seeded delays, drops, partitions, crashes, invariant checks |
| `lincheck/` | the Wing and Gong search, with its own known-answer tests |
| `kv/` | the replicated key-value store and the sequential model it is checked against |
| `cmd/simtest/` | the driver: clients, fault profiles, the defect matrix |
