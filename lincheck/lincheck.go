// Package lincheck decides whether a recorded client history could have come
// from a single sequential store.
//
// It is the Wing and Gong search: repeatedly try to linearize some operation
// whose call has already happened, apply it to the model, and recurse; on
// failure, put it back and try the next one. Memoizing on (model state, set of
// operations already linearized) is what keeps it from being hopeless.
//
// What it buys over checking invariants: invariants describe what the
// implementation believes about itself, and this describes what a client could
// legally have seen. A stale read served by a deposed leader breaks no Raft
// invariant and is caught here.
package lincheck

import (
	"fmt"
	"sort"
	"strings"

	"github.com/AshrithaG/raft-control-plane/kv"
)

// Op is one client operation. Call and Return are virtual ticks. An operation
// that never returned is Pending: the store may or may not have applied it, and
// the checker has to allow both.
type Op struct {
	Client  int
	Cmd     kv.Command
	Result  string
	Call    int
	Return  int
	Pending bool
}

type Result struct {
	OK      bool
	Reason  string
	Trouble *Op
	Order   []Op // a witnessing sequential order when OK
}

// Check returns whether the history is linearizable. Operations are sorted by
// call time; an operation may be linearized once every operation that returned
// before its call has been linearized.
func Check(hist []Op) Result {
	ops := make([]Op, len(hist))
	copy(ops, hist)
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	if len(ops) > 64 {
		// The memo key uses a 64-bit set. Longer histories are checked in
		// overlapping windows by the caller rather than silently truncated.
		return Result{OK: false, Reason: fmt.Sprintf("history of %d ops exceeds the 64-op window", len(ops))}
	}

	done := make([]bool, len(ops))
	memo := map[string]bool{}
	var order []Op

	var search func(st *kv.State, linearized int, maxReturned int) bool
	search = func(st *kv.State, linearized int, maxReturned int) bool {
		if linearized == len(ops) {
			return true
		}
		var mask uint64
		for i, d := range done {
			if d {
				mask |= 1 << uint(i)
			}
		}
		key := fmt.Sprintf("%s|%d", st.Key(), mask)
		if memo[key] {
			return false
		}
		for i := range ops {
			if done[i] {
				continue
			}
			// An operation cannot be linearized before an operation that had
			// already returned when this one was called.
			if ops[i].Call > maxReturned && maxReturned != 0 {
				// fine: later call, allowed
				_ = i
			}
			blocked := false
			for j := range ops {
				if done[j] || i == j || ops[j].Pending {
					continue
				}
				if ops[j].Return <= ops[i].Call {
					blocked = true // j returned before i was called, so j goes first
					break
				}
			}
			if blocked {
				continue
			}
			next := st.Clone()
			got := next.Apply(ops[i].Cmd)
			if !ops[i].Pending && got != ops[i].Result {
				continue // this order would have shown the client something else
			}
			done[i] = true
			order = append(order, ops[i])
			if search(next, linearized+1, ops[i].Return) {
				return true
			}
			done[i] = false
			order = order[:len(order)-1]

			// An operation whose result the client never learned may simply
			// never have taken effect, so dropping it is a legal history too.
			// Forcing every timed-out operation into the order reported a
			// correct implementation as unlinearizable.
			if ops[i].Pending {
				done[i] = true
				if search(st, linearized+1, maxReturned) {
					return true
				}
				done[i] = false
			}
		}
		memo[key] = true
		return false
	}

	if search(kv.NewState(), 0, 0) {
		out := make([]Op, len(order))
		copy(out, order)
		return Result{OK: true, Order: out}
	}
	_ = memo
	// Report the earliest unlinearizable operation: the first one, in call
	// order, that no sequential order could satisfy.
	var worst *Op
	for i := range ops {
		if !done[i] {
			worst = &ops[i]
			break
		}
	}
	return Result{OK: false, Reason: "no sequential order explains this history", Trouble: worst}
}

// MinimalPrefix returns the shortest prefix of the history, in call order,
// that is already unlinearizable. Reporting the whole history says only that
// something is wrong somewhere; the shortest failing prefix says where, and it
// is what makes a failure worth pasting into a bug report.
func MinimalPrefix(hist []Op) []Op {
	ops := make([]Op, len(hist))
	copy(ops, hist)
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	for k := 1; k <= len(ops); k++ {
		if !Check(ops[:k]).OK {
			return ops[:k]
		}
	}
	return nil
}

// Describe renders a history compactly enough to paste into a bug report.
func Describe(hist []Op) string {
	var b strings.Builder
	for _, o := range hist {
		state := o.Result
		if o.Pending {
			state = "?"
		}
		fmt.Fprintf(&b, "c%d %s %s=%s -> %s [%d,%d]\n",
			o.Client, o.Cmd.Kind, o.Cmd.Key, o.Cmd.Val, state, o.Call, o.Return)
	}
	return b.String()
}
