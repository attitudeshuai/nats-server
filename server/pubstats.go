// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// defaultPublishStatsMaxSubjects is the per-account subject-entry capacity
// used when publish_stats is enabled without an explicit capacity.
const defaultPublishStatsMaxSubjects = 1024

// PublishStatsOpts configures the optional publish-side, per-account and
// per-subject counting ledger. It is disabled by default and, when disabled,
// the publish hot path performs a single nil check and allocates nothing.
//
// The ledger is in-memory only: counts always start at zero on (re)start and
// are never persisted. It is intended for operational diagnostics, not
// billing.
type PublishStatsOpts struct {
	// Enabled turns the per-account/per-subject publish ledger on.
	Enabled bool `json:"enabled,omitempty"`
	// MaxSubjectsPerAccount bounds the number of tracked subject entries per
	// account. A value of 0 means defaultPublishStatsMaxSubjects. Once the
	// bound is reached, coldest subjects are deterministically merged into an
	// aggregate bucket and an overflow marker is exposed.
	MaxSubjectsPerAccount int `json:"max_subjects_per_account,omitempty"`
}

// capacity returns the effective per-account subject capacity. A zero value
// means the default capacity is requested.
func (p *PublishStatsOpts) capacity() int {
	if p == nil || p.MaxSubjectsPerAccount <= 0 {
		return defaultPublishStatsMaxSubjects
	}
	return p.MaxSubjectsPerAccount
}

// PubStatsCounters is a pair of message/byte counters exposed by /pubstatz.
type PubStatsCounters struct {
	Msgs  int64 `json:"msgs"`
	Bytes int64 `json:"bytes"`
}

// PubStatsSubject is one retained per-subject counter row.
type PubStatsSubject struct {
	Subject string `json:"subject"`
	Msgs    int64  `json:"msgs"`
	Bytes   int64  `json:"bytes"`
}

// PubStatsAccount is the per-account cross-section exposed by /pubstatz.
type PubStatsAccount struct {
	// Account is the account name.
	Account string `json:"account"`
	// Overflow is a sticky marker set once subject eviction has occurred for
	// this account; it tells readers the retained Subjects rows are not a
	// complete picture. Cleared only by a reset.
	Overflow bool `json:"overflow"`
	// EvictedSubjects accumulates the number of retained subject entries that
	// have been merged into Other.
	EvictedSubjects uint64 `json:"evicted_subjects"`
	// RetainedSubjects is the current number of tracked subject entries; it
	// never exceeds the configured capacity.
	RetainedSubjects int `json:"retained_subjects"`
	// Other is the aggregate bucket holding counters of every evicted
	// subject entry.
	Other PubStatsCounters `json:"other"`
	// Subjects contains the retained per-subject counters, ordered by msgs
	// desc, then bytes desc, then subject asc.
	Subjects []PubStatsSubject `json:"subjects"`
}

// pubSubjectCounters are the atomic counters of one retained subject entry.
type pubSubjectCounters struct {
	msgs  int64
	bytes int64
}

// pubSubjectLedger is the bounded, per-account subject ledger.
//
// All fields are guarded by mu, except that increments on an existing entry's
// counters use atomic operations while holding mu.RLock. Because every
// increment on an entry is performed while holding mu.RLock, a writer
// (eviction/reset) only runs once all in-flight increments have drained, so
// eviction merges exact values and resets never lose in-flight updates.
type pubSubjectLedger struct {
	mu         sync.RWMutex
	capacity   int
	m          map[string]*pubSubjectCounters
	overflow   bool
	otherMsgs  int64
	otherBytes int64
	evicted    uint64
	resetTime  time.Time
}

func newPubSubjectLedger(capacity int) *pubSubjectLedger {
	if capacity <= 0 {
		capacity = defaultPublishStatsMaxSubjects
	}
	return &pubSubjectLedger{
		capacity: capacity,
		m:        make(map[string]*pubSubjectCounters, capacity),
	}
}

// record adds one publish to the given subject. Callers must hold the manager
// lease RLock. The fast path only takes mu.RLock and performs atomic adds;
// the slow path creates the entry (and deterministically evicts a coldest
// batch when the map is at capacity).
func (l *pubSubjectLedger) record(subj string, msgBytes int64) {
	l.mu.RLock()
	if e, ok := l.m[subj]; ok {
		atomic.AddInt64(&e.msgs, 1)
		atomic.AddInt64(&e.bytes, msgBytes)
		l.mu.RUnlock()
		return
	}
	l.mu.RUnlock()

	l.mu.Lock()
	defer l.mu.Unlock()
	// Re-check under the write lock; another recorder may have won.
	if e, ok := l.m[subj]; ok {
		atomic.AddInt64(&e.msgs, 1)
		atomic.AddInt64(&e.bytes, msgBytes)
		return
	}
	if len(l.m) >= l.capacity {
		l.evictLocked()
	}
	l.m[subj] = &pubSubjectCounters{msgs: 1, bytes: msgBytes}
}

// evictLocked merges a deterministic coldest batch of retained entries into
// the aggregate bucket and flips the sticky overflow marker. Selection order:
// msgs asc, bytes asc, subject lex asc. Caller holds mu.Lock.
func (l *pubSubjectLedger) evictLocked() {
	batch := l.capacity / 10
	if batch < 1 {
		batch = 1
	}
	if batch > len(l.m) {
		batch = len(l.m)
	}

	type candidate struct {
		subj  string
		msgs  int64
		bytes int64
	}
	cands := make([]candidate, 0, len(l.m))
	for s, e := range l.m {
		cands = append(cands, candidate{
			subj:  s,
			msgs:  atomic.LoadInt64(&e.msgs),
			bytes: atomic.LoadInt64(&e.bytes),
		})
	}
	slices.SortFunc(cands, func(a, b candidate) int {
		if c := cmp.Compare(a.msgs, b.msgs); c != 0 {
			return c
		}
		if c := cmp.Compare(a.bytes, b.bytes); c != 0 {
			return c
		}
		return cmp.Compare(a.subj, b.subj)
	})

	for i := 0; i < batch; i++ {
		c := cands[i]
		l.otherMsgs += c.msgs
		l.otherBytes += c.bytes
		delete(l.m, c.subj)
	}
	l.evicted += uint64(batch)
	l.overflow = true
}

// resetLocked zeroes the ledger. Caller holds mu.Lock (and the manager lease
// Lock), which guarantees every in-flight record has drained. The returned
// view is the exact, closed pre-reset era cross-section.
func (l *pubSubjectLedger) resetLocked(now time.Time) *PubStatsAccount {
	era := l.viewLocked("")
	l.m = make(map[string]*pubSubjectCounters, l.capacity)
	l.overflow = false
	l.otherMsgs = 0
	l.otherBytes = 0
	l.evicted = 0
	l.resetTime = now
	return era
}

// viewLocked builds the external view for the ledger. Caller holds mu (R or
// Lock). Subjects are copied and sorted, so the result is safe to use after
// the lock is released.
func (l *pubSubjectLedger) viewLocked(account string) *PubStatsAccount {
	out := &PubStatsAccount{
		Account:          account,
		Overflow:         l.overflow,
		EvictedSubjects:  l.evicted,
		RetainedSubjects: len(l.m),
		Other: PubStatsCounters{
			Msgs:  l.otherMsgs,
			Bytes: l.otherBytes,
		},
		Subjects: make([]PubStatsSubject, 0, len(l.m)),
	}
	for s, e := range l.m {
		out.Subjects = append(out.Subjects, PubStatsSubject{
			Subject: s,
			Msgs:    atomic.LoadInt64(&e.msgs),
			Bytes:   atomic.LoadInt64(&e.bytes),
		})
	}
	slices.SortFunc(out.Subjects, func(a, b PubStatsSubject) int {
		if c := cmp.Compare(b.Msgs, a.Msgs); c != 0 {
			return c
		}
		if c := cmp.Compare(b.Bytes, a.Bytes); c != 0 {
			return c
		}
		return cmp.Compare(a.Subject, b.Subject)
	})
	return out
}

// pubStatsMgr owns the per-account ledgers and the publish lease.
//
// The lease is a readers-writers lock taken for the full duration of every
// record (RLock) and every snapshot (RLock). A global reset takes Lock, so it
// waits for all in-flight records/snapshots to finish and blocks new ones
// until the whole reset is applied: updates in flight at the reset boundary
// linearize exactly once (pre-reset era or post-reset era), and a snapshot can
// never observe a half-reset state.
type pubStatsMgr struct {
	// lease protects ledgers structure membership across record/snapshot/
	// reset/remove.
	lease   sync.RWMutex
	ledgers sync.Map // map[*Account]*pubSubjectLedger

	capacity  int
	resetTime time.Time // guarded by lease
}

func newPubStatsMgr(capacity int) *pubStatsMgr {
	if capacity <= 0 {
		capacity = defaultPublishStatsMaxSubjects
	}
	return &pubStatsMgr{capacity: capacity}
}

// record accounts one accepted local ingress publish. It never blocks on a
// concurrent snapshot (both hold lease.RLock); only reset/remove (lease.Lock)
// or per-ledger structural changes (new subject / eviction) take exclusive
// locks briefly.
func (m *pubStatsMgr) record(acc *Account, subj string, msgBytes int64) {
	if acc == nil || subj == "" {
		return
	}
	m.lease.RLock()
	defer m.lease.RUnlock()

	// Load first so the hot path never allocates when the ledger exists.
	v, ok := m.ledgers.Load(acc)
	if !ok {
		v, _ = m.ledgers.LoadOrStore(acc, newPubSubjectLedger(m.capacity))
	}
	v.(*pubSubjectLedger).record(subj, msgBytes)
}

// snapshot returns a self-consistent cross-section of all ledgers.
//
// wanted (when non-nil) restricts the output to the named accounts; existing
// reports which of those names currently exist server-side. Existing accounts
// with no traffic yet get a zero view without creating a ledger. The reset
// time returned is the generation timestamp of the cross-section.
func (m *pubStatsMgr) snapshot(wanted map[string]struct{}, existing map[string]struct{}) ([]*PubStatsAccount, time.Time) {
	m.lease.RLock()
	defer m.lease.RUnlock()

	seen := make(map[string]struct{}, len(wanted))
	var out []*PubStatsAccount
	m.ledgers.Range(func(k, v any) bool {
		acc, l := k.(*Account), v.(*pubSubjectLedger)
		if wanted != nil {
			if _, ok := wanted[acc.Name]; !ok {
				return true
			}
			seen[acc.Name] = struct{}{}
		}
		l.mu.RLock()
		out = append(out, l.viewLocked(acc.Name))
		l.mu.RUnlock()
		return true
	})

	if wanted != nil {
		for name := range wanted {
			if _, ok := seen[name]; ok {
				continue
			}
			// Emit a zero view only when the account exists server-side;
			// this does not create a ledger.
			if _, ok := existing[name]; ok {
				out = append(out, newZeroPubStatsAccount(name))
			}
		}
	}

	slices.SortFunc(out, func(a, b *PubStatsAccount) int {
		return cmp.Compare(a.Account, b.Account)
	})
	return out, m.resetTime
}

// reset atomically zeroes every account ledger. It returns the reset
// timestamp and the exact pre-reset (closed era) cross-section; summing eras
// and the current cross-section reconciles to every recorded publish.
func (m *pubStatsMgr) reset() (time.Time, []*PubStatsAccount) {
	m.lease.Lock()
	defer m.lease.Unlock()

	now := time.Now().UTC()
	m.resetTime = now

	era := make([]*PubStatsAccount, 0)
	m.ledgers.Range(func(k, v any) bool {
		acc, l := k.(*Account), v.(*pubSubjectLedger)
		l.mu.Lock()
		a := l.resetLocked(now)
		a.Account = acc.Name
		l.mu.Unlock()
		era = append(era, a)
		return true
	})
	slices.SortFunc(era, func(a, b *PubStatsAccount) int {
		return cmp.Compare(a.Account, b.Account)
	})
	return now, era
}

// remove drops the ledger of a deleted account. Ledgers are keyed by *Account
// pointer, so a recreated account of the same name starts from zero.
func (m *pubStatsMgr) remove(acc *Account) {
	if acc == nil {
		return
	}
	m.lease.Lock()
	defer m.lease.Unlock()
	m.ledgers.LoadAndDelete(acc)
}

// newZeroPubStatsAccount returns an empty view for an existing account that
// has not yet produced traffic.
func newZeroPubStatsAccount(name string) *PubStatsAccount {
	return &PubStatsAccount{
		Account:  name,
		Subjects: []PubStatsSubject{},
	}
}

// accountPublishStats records one accepted local-ingress publish into the
// optional ledger. When the feature is disabled this is a single nil-pointer
// check with no allocation, no lock and no string conversion at call sites
// that pass the subject only when non-nil.
func (s *Server) accountPublishStats(acc *Account, subj string, msgBytes int64) {
	if m := s.pubStats.Load(); m != nil {
		m.record(acc, subj, msgBytes)
	}
}

// applyPublishStats reconciles the ledger manager with a new configuration.
// Disabling removes the manager; enabling (or changing the capacity) installs
// a fresh zero-state manager so a new configuration round never inherits the
// counts of the previous one; enabling with an unchanged effective capacity
// preserves the running ledger.
func (s *Server) applyPublishStats(prev, next PublishStatsOpts) {
	switch {
	case !next.Enabled:
		s.pubStats.Store(nil)
	case !prev.Enabled || prev.capacity() != next.capacity():
		s.pubStats.Store(newPubStatsMgr(next.capacity()))
	}
}
