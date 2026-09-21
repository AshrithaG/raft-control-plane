// Package kv is the replicated state machine the Raft cluster agrees on, and
// the sequential model the linearizability checker compares against.
package kv

import (
	"encoding/json"
	"fmt"
)

type Kind string

const (
	Put Kind = "put"
	Get Kind = "get"
	CAS Kind = "cas"
)

// Command is what goes in the log. It carries a client and sequence number so
// a command redelivered after a retry is applied once, which is what makes the
// KV linearizable rather than merely consistent.
type Command struct {
	Client int    `json:"c"`
	Seq    int    `json:"s"`
	Kind   Kind   `json:"k"`
	Key    string `json:"key"`
	Val    string `json:"v,omitempty"`
	Old    string `json:"o,omitempty"`
}

func (c Command) Encode() []byte { b, _ := json.Marshal(c); return b }

func Decode(b []byte) (Command, bool) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, false
	}
	return c, true
}

// State is a sequential key-value store with per-client dedup.
type State struct {
	data map[string]string
	seen map[int]int    // client -> highest applied seq
	last map[int]string // client -> that command's result
}

func NewState() *State {
	return &State{data: map[string]string{}, seen: map[int]int{}, last: map[int]string{}}
}

func (s *State) Clone() *State {
	c := NewState()
	for k, v := range s.data {
		c.data[k] = v
	}
	for k, v := range s.seen {
		c.seen[k] = v
	}
	for k, v := range s.last {
		c.last[k] = v
	}
	return c
}

// Apply runs one command and returns its result. A repeat of an already applied
// (client, seq) returns the original result instead of applying twice.
func (s *State) Apply(c Command) string {
	if prev, ok := s.seen[c.Client]; ok && c.Seq <= prev && c.Kind != Get {
		return s.last[c.Client]
	}
	var res string
	switch c.Kind {
	case Put:
		s.data[c.Key] = c.Val
		res = "ok"
	case Get:
		res = s.data[c.Key]
	case CAS:
		if s.data[c.Key] == c.Old {
			s.data[c.Key] = c.Val
			res = "true"
		} else {
			res = "false"
		}
	}
	if c.Kind != Get {
		s.seen[c.Client] = c.Seq
		s.last[c.Client] = res
	}
	return res
}

// Key is a canonical encoding of the store, used to memoize search states in
// the linearizability checker.
func (s *State) Key() string {
	return fmt.Sprintf("%v", s.data)
}

// Get is a read-only lookup, used by the local read path after a read barrier
// has been confirmed.
func (s *State) Get(key string) string { return s.data[key] }
