// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func parseTempConfig(t *testing.T, body string) *Options {
	t.Helper()
	dir := t.TempDir()
	f := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(f, []byte(body), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	opts, err := ProcessConfigFile(f)
	if err != nil {
		t.Fatalf("ProcessConfigFile failed: %v", err)
	}
	return opts
}

func TestPublishStatsConfigAbsent(t *testing.T) {
	opts := parseTempConfig(t, `port: -1`)
	if opts.PublishStats.Enabled {
		t.Fatalf("expected disabled by default, got %+v", opts.PublishStats)
	}
	if opts.PublishStats.MaxSubjectsPerAccount != 0 {
		t.Fatalf("expected zero capacity (default-at-build), got %d", opts.PublishStats.MaxSubjectsPerAccount)
	}
}

func TestPublishStatsConfigBlock(t *testing.T) {
	// enabled with explicit capacity.
	opts := parseTempConfig(t, `
port: -1
publish_stats: {
  enabled: true
  max_subjects_per_account: 16
}`)
	if !opts.PublishStats.Enabled {
		t.Fatalf("expected enabled")
	}
	if opts.PublishStats.MaxSubjectsPerAccount != 16 {
		t.Fatalf("expected capacity 16, got %d", opts.PublishStats.MaxSubjectsPerAccount)
	}

	// enabled without capacity: zero means default-at-build.
	opts = parseTempConfig(t, `
port: -1
publish_stats: { enabled: true }`)
	if !opts.PublishStats.Enabled || opts.PublishStats.MaxSubjectsPerAccount != 0 {
		t.Fatalf("unexpected: %+v", opts.PublishStats)
	}
	if c := opts.PublishStats.capacity(); c != defaultPublishStatsMaxSubjects {
		t.Fatalf("expected default capacity %d, got %d", defaultPublishStatsMaxSubjects, c)
	}

	// explicit zero keeps the default semantics.
	opts = parseTempConfig(t, `
port: -1
publish_stats: { enabled: true, max_subjects_per_account: 0 }`)
	if !opts.PublishStats.Enabled || opts.PublishStats.MaxSubjectsPerAccount != 0 {
		t.Fatalf("unexpected: %+v", opts.PublishStats)
	}

	// boolean shorthand.
	opts = parseTempConfig(t, `
port: -1
publish_stats: true`)
	if !opts.PublishStats.Enabled {
		t.Fatalf("expected enabled via bool shorthand")
	}

	opts = parseTempConfig(t, `
port: -1
publish_stats: false`)
	if opts.PublishStats.Enabled {
		t.Fatalf("expected disabled via bool shorthand")
	}
}

func TestPublishStatsConfigErrors(t *testing.T) {
	cases := map[string]string{
		"negative capacity":    `publish_stats: { enabled: true, max_subjects_per_account: -1 }`,
		"non-integer capacity": `publish_stats: { enabled: true, max_subjects_per_account: "16" }`,
		"non-bool enabled":     `publish_stats: { enabled: "true" }`,
		"unknown field":        `publish_stats: { enabled: true, bogus: 1 }`,
		"unsupported scalar":   `publish_stats: 1234`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "nats.conf")
			if err := os.WriteFile(f, []byte(body), 0644); err != nil {
				t.Fatalf("write config: %v", err)
			}
			if _, err := ProcessConfigFile(f); err == nil {
				t.Fatalf("expected config error for %q", name)
			}
		})
	}

	// Capacity is ignored (no error) when the feature is disabled.
	opts := parseTempConfig(t, `
port: -1
publish_stats: { enabled: false, max_subjects_per_account: -1 }`)
	if opts.PublishStats.Enabled || opts.PublishStats.MaxSubjectsPerAccount != 0 {
		t.Fatalf("expected disabled with ignored capacity, got %+v", opts.PublishStats)
	}
}

func pubView(mgr *pubStatsMgr, account string) *PubStatsAccount {
	views, _ := mgr.snapshot(nil, nil)
	for _, v := range views {
		if v.Account == account {
			return v
		}
	}
	return nil
}

func pubTotals(v *PubStatsAccount) (int64, int64) {
	var msgs, bytes int64
	for _, s := range v.Subjects {
		msgs += s.Msgs
		bytes += s.Bytes
	}
	msgs += v.Other.Msgs
	bytes += v.Other.Bytes
	return msgs, bytes
}

// Within capacity every subject is retained individually and ordering is
// msgs desc, bytes desc, subject asc.
func TestPubSubjectLedgerWithinCapacity(t *testing.T) {
	mgr := newPubStatsMgr(8)
	acc := &Account{Name: "acc"}

	mgr.record(acc, "a.single", 7) // 1 x 7
	mgr.record(acc, "a.single", 3) // 2 x 10
	mgr.record(acc, "b.double", 5) // 1
	mgr.record(acc, "b.double", 5) // 2
	mgr.record(acc, "c.triple", 2) // 3
	mgr.record(acc, "c.triple", 2) // 3
	mgr.record(acc, "c.triple", 2) // 3

	v := pubView(mgr, "acc")
	if v == nil {
		t.Fatal("missing account view")
	}
	if v.Overflow || v.EvictedSubjects != 0 || v.Other.Msgs != 0 || v.Other.Bytes != 0 {
		t.Fatalf("unexpected overflow state: %+v", v)
	}
	if v.RetainedSubjects != 3 || len(v.Subjects) != 3 {
		t.Fatalf("expected 3 retained subjects, got %d", v.RetainedSubjects)
	}
	want := []PubStatsSubject{
		{Subject: "c.triple", Msgs: 3, Bytes: 6},
		{Subject: "a.single", Msgs: 2, Bytes: 10},
		{Subject: "b.double", Msgs: 2, Bytes: 10},
	}
	// a.single vs b.double: tie msgs=2, bytes=10 -> subject asc.
	if v.Subjects[0] != want[0] {
		t.Fatalf("first row mismatch: got %+v want %+v", v.Subjects[0], want[0])
	}
	got := map[string]PubStatsSubject{}
	for _, s := range v.Subjects {
		got[s.Subject] = s
	}
	for _, w := range want {
		if got[w.Subject] != w {
			t.Fatalf("row %q mismatch: got %+v want %+v", w.Subject, got[w.Subject], w)
		}
	}
	if msgs, bytes := pubTotals(v); msgs != 7 || bytes != 26 {
		t.Fatalf("totals mismatch: %d/%d", msgs, bytes)
	}
}

// At capacity, the deterministic coldest entry (msgs asc, bytes asc, subject
// asc) is merged into the aggregate bucket and overflow becomes sticky.
func TestPubSubjectLedgerEviction(t *testing.T) {
	run := func() *pubStatsMgr {
		mgr := newPubStatsMgr(5)
		acc := &Account{Name: "acc"}
		// Five distinct subjects with ascending counts (coldest is "s.1").
		for i := 1; i <= 5; i++ {
			for j := 0; j < i; j++ {
				mgr.record(acc, fmt.Sprintf("s.%d", i), int64(i))
			}
		}
		// Sixth distinct subject triggers a batch=1 eviction.
		mgr.record(acc, "s.new", 99)
		return mgr
	}

	a := run()
	b := run()
	va, vb := pubView(a, "acc"), pubView(b, "acc")

	for i, v := range []*PubStatsAccount{va, vb} {
		if !v.Overflow {
			t.Fatalf("run %d: overflow not set", i)
		}
		if v.EvictedSubjects != 1 {
			t.Fatalf("run %d: expected 1 evicted, got %d", i, v.EvictedSubjects)
		}
		if v.RetainedSubjects > 5 {
			t.Fatalf("run %d: retained %d exceeds capacity", i, v.RetainedSubjects)
		}
		if v.Other.Msgs != 1 || v.Other.Bytes != 1 {
			t.Fatalf("run %d: aggregate bucket mismatch: %+v", i, v.Other)
		}
		if msgs, bytes := pubTotals(v); msgs != 16 || bytes != (1+4+9+16+25)+99 {
			t.Fatalf("run %d: totals after eviction mismatch: %d/%d", i, msgs, bytes)
		}
		// Hot subjects survive exactly.
		rows := map[string]PubStatsSubject{}
		for _, s := range v.Subjects {
			rows[s.Subject] = s
		}
		if _, ok := rows["s.1"]; ok {
			t.Fatalf("run %d: coldest subject should have been evicted", i)
		}
		if hot := rows["s.5"]; hot.Msgs != 5 || hot.Bytes != 25 {
			t.Fatalf("run %d: hot subject counters wrong: %+v", i, hot)
		}
		if fresh := rows["s.new"]; fresh.Msgs != 1 || fresh.Bytes != 99 {
			t.Fatalf("run %d: new subject counters wrong: %+v", i, fresh)
		}
	}

	// Deterministic: both runs must expose the identical retained set.
	if len(va.Subjects) != len(vb.Subjects) {
		t.Fatal("eviction non-deterministic: length differs")
	}
	for i := range va.Subjects {
		if va.Subjects[i] != vb.Subjects[i] {
			t.Fatalf("eviction non-deterministic at %d: %+v vs %+v", i, va.Subjects[i], vb.Subjects[i])
		}
	}
}

// An evicted subject that publishes again re-enters as a fresh entry; its
// historical counters remain in the aggregate bucket.
func TestPubSubjectLedgerEvictedSubjectReenters(t *testing.T) {
	mgr := newPubStatsMgr(2)
	acc := &Account{Name: "acc"}
	mgr.record(acc, "a", 1)
	mgr.record(acc, "b", 10)
	mgr.record(acc, "c", 1) // evict coldest "a" (tie msgs, bytes 1 vs 10): other {1,1}.
	mgr.record(acc, "a", 5) // full again; evict coldest "c" (1,1): other {2,2}; "a" re-enters fresh.

	v := pubView(mgr, "acc")
	rows := map[string]PubStatsSubject{}
	for _, s := range v.Subjects {
		rows[s.Subject] = s
	}
	if rows["a"].Msgs != 1 || rows["a"].Bytes != 5 {
		t.Fatalf("re-entered subject should be fresh, got %+v", rows["a"])
	}
	if v.Other.Msgs != 2 || v.Other.Bytes != 2 {
		t.Fatalf("history of both evicted entries should be in other: %+v", v.Other)
	}
	if msgs, bytes := pubTotals(v); msgs != 4 || bytes != 17 {
		t.Fatalf("totals mismatch: %d/%d", msgs, bytes)
	}
}

// Same-subject and cross-subject concurrent publishes must never lose or
// double-count.
func TestPubSubjectLedgerConcurrent(t *testing.T) {
	mgr := newPubStatsMgr(256)
	acc := &Account{Name: "acc"}
	const (
		goroutines = 8
		publishes  = 5000
		subjectsN  = 16
	)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < publishes; i++ {
				subj := fmt.Sprintf("s.%d", (g+i)%subjectsN)
				mgr.record(acc, subj, int64(1+(i%7)))
			}
		}(g)
	}
	wg.Wait()

	v := pubView(mgr, "acc")
	totalMsgs, totalBytes := pubTotals(v)
	if totalMsgs != goroutines*publishes {
		t.Fatalf("msgs lost/duplicated: got %d want %d", totalMsgs, goroutines*publishes)
	}
	var wantBytes int64
	for g := 0; g < goroutines; g++ {
		for i := 0; i < publishes; i++ {
			wantBytes += int64(1 + (i % 7))
		}
	}
	if totalBytes != wantBytes {
		t.Fatalf("bytes lost/duplicated: got %d want %d", totalBytes, wantBytes)
	}
}

// Reset eras must reconcile exactly: every record lands in exactly one era,
// even when records and resets run concurrently.
func TestPubStatsMgrResetConcurrent(t *testing.T) {
	mgr := newPubStatsMgr(128)
	acc := &Account{Name: "acc"}
	const (
		goroutines = 8
		publishes  = 4000
	)
	var (
		wg, rwg  sync.WaitGroup
		stop     = make(chan struct{})
		eraMu    sync.Mutex
		eraMsgs  int64
		eraBytes int64
	)
	var wantBytes int64
	for g := 0; g < goroutines; g++ {
		for i := 0; i < publishes; i++ {
			wantBytes += int64(1 + (i % 7))
		}
	}

	rwg.Add(1)
	go func() {
		defer rwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, era := mgr.reset()
				var m, b int64
				for _, a := range era {
					mm, bb := pubTotals(a)
					m += mm
					b += bb
				}
				eraMu.Lock()
				eraMsgs += m
				eraBytes += b
				eraMu.Unlock()
				runtime.Gosched()
			}
		}
	}()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < publishes; i++ {
				mgr.record(acc, fmt.Sprintf("s.%d", i%32), int64(1+(i%7)))
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	rwg.Wait()

	// Final era closes whatever remained; current cross-section must be zero.
	_, finalEra := mgr.reset()
	for _, a := range finalEra {
		m, b := pubTotals(a)
		eraMsgs += m
		eraBytes += b
	}

	if eraMsgs != goroutines*publishes {
		t.Fatalf("reset-era reconciliation failed for msgs: got %d want %d", eraMsgs, goroutines*publishes)
	}
	if eraBytes != wantBytes {
		t.Fatalf("reset-era reconciliation failed for bytes: got %d want %d", eraBytes, wantBytes)
	}

	cur := pubView(mgr, "acc")
	if cur == nil {
		t.Fatal("expected zero view to still enumerate ledger")
	}
	if cur.Overflow || cur.EvictedSubjects != 0 || cur.Other.Msgs != 0 || cur.Other.Bytes != 0 || len(cur.Subjects) != 0 {
		t.Fatalf("post-reset state not zero: %+v", cur)
	}
}

// Snapshot cross-sections stay structurally intact while churn continuously
// creates and evicts subjects.
func TestPubStatsMgrSnapshotUnderChurn(t *testing.T) {
	mgr := newPubStatsMgr(16)
	acc := &Account{Name: "acc"}
	const writers = 6
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				mgr.record(acc, fmt.Sprintf("w%d.s%d", w, i%200), int64(i%5+1))
			}
		}(w)
	}

	for i := 0; i < 500; i++ {
		views, rt := mgr.snapshot(nil, nil)
		for _, v := range views {
			if v.RetainedSubjects > 16 {
				t.Fatalf("retained %d exceeds capacity", v.RetainedSubjects)
			}
			seen := make(map[string]struct{}, len(v.Subjects))
			for _, s := range v.Subjects {
				if _, dup := seen[s.Subject]; dup {
					t.Fatalf("duplicate subject in snapshot: %q", s.Subject)
				}
				seen[s.Subject] = struct{}{}
				if s.Msgs <= 0 || s.Bytes <= 0 {
					t.Fatalf("non-positive counters in snapshot: %+v", s)
				}
			}
			b, err := json.Marshal(v)
			if err != nil {
				t.Fatalf("snapshot not JSON serializable: %v", err)
			}
			var back PubStatsAccount
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatalf("snapshot JSON not parseable: %v", err)
			}
			if back.RetainedSubjects != v.RetainedSubjects {
				t.Fatalf("round-trip mismatch")
			}
		}
		_ = rt
		runtime.Gosched()
	}
	close(stop)
	wg.Wait()
}

// Account filtering: unknown names ignored; existing-but-quiet accounts get a
// zero view without creating a ledger.
func TestPubStatsMgrSnapshotFilter(t *testing.T) {
	mgr := newPubStatsMgr(16)
	accA := &Account{Name: "A"}
	mgr.record(accA, "x", 1)

	existing := map[string]struct{}{"A": {}, "QUIET": {}}
	wanted := map[string]struct{}{"A": {}, "QUIET": {}, "GONE": {}}
	views, _ := mgr.snapshot(wanted, existing)
	byName := map[string]*PubStatsAccount{}
	for _, v := range views {
		byName[v.Account] = v
	}
	if _, ok := byName["GONE"]; ok {
		t.Fatal("unknown account should be ignored")
	}
	a, ok := byName["A"]
	if !ok || len(a.Subjects) != 1 {
		t.Fatalf("account A view wrong: %+v", a)
	}
	q, ok := byName["QUIET"]
	if !ok || len(q.Subjects) != 0 || q.Overflow || q.EvictedSubjects != 0 {
		t.Fatalf("quiet account zero view wrong: %+v", q)
	}
	// The zero view must not have materialized a ledger: only accA's ledger
	// may exist.
	n := 0
	mgr.ledgers.Range(func(k, _ any) bool {
		n++
		if k.(*Account) != accA {
			t.Fatalf("unexpected ledger for %v", k)
		}
		return true
	})
	if n != 1 {
		t.Fatalf("snapshot must not create ledgers, found %d", n)
	}
}

// Removing an account drops its ledger; a recreated account of the same name
// starts from zero.
func TestPubStatsMgrRemove(t *testing.T) {
	mgr := newPubStatsMgr(16)
	acc := &Account{Name: "A"}
	mgr.record(acc, "x", 1)
	mgr.remove(acc)
	if v := pubView(mgr, "A"); v != nil {
		t.Fatalf("expected ledger removed, got %+v", v)
	}
	recreated := &Account{Name: "A"}
	mgr.record(recreated, "y", 2)
	v := pubView(mgr, "A")
	if v == nil || len(v.Subjects) != 1 || v.Subjects[0].Subject != "y" {
		t.Fatalf("recreated account must start from zero: %+v", v)
	}
}

// applyPublishStats transition rules (also used by config reload).
func TestServerApplyPublishStats(t *testing.T) {
	s := &Server{}

	// disabled -> enabled installs a fresh zero-state manager.
	s.applyPublishStats(PublishStatsOpts{}, PublishStatsOpts{Enabled: true})
	m1 := s.pubStats.Load()
	if m1 == nil || m1.capacity != defaultPublishStatsMaxSubjects {
		t.Fatalf("enable failed: %+v", m1)
	}

	// enabled, same capacity: manager preserved.
	s.applyPublishStats(PublishStatsOpts{Enabled: true}, PublishStatsOpts{Enabled: true})
	if s.pubStats.Load() != m1 {
		t.Fatal("unchanged config must preserve the manager")
	}

	// enabled, changed capacity: fresh zero-state manager.
	s.applyPublishStats(PublishStatsOpts{Enabled: true}, PublishStatsOpts{Enabled: true, MaxSubjectsPerAccount: 8})
	m2 := s.pubStats.Load()
	if m2 == nil || m2 == m1 || m2.capacity != 8 {
		t.Fatalf("capacity change must install a fresh manager: m1=%p m2=%p cap=%d", m1, m2, m2.capacity)
	}

	// enabled -> disabled: removed.
	s.applyPublishStats(PublishStatsOpts{Enabled: true, MaxSubjectsPerAccount: 8}, PublishStatsOpts{})
	if s.pubStats.Load() != nil {
		t.Fatal("disable must remove the manager")
	}
}

// End-to-end: accepted client publishes land in the ledger per account and
// subject with exact msgs/bytes, and the disabled server has no ledger.
func TestServerPublishStatsHook(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.PublishStats.Enabled = true
	opts.PublishStats.MaxSubjectsPerAccount = 64
	s := RunServer(&opts)
	defer s.Shutdown()

	if s.pubStats.Load() == nil {
		t.Fatal("ledger manager should be installed")
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	// Subjects with and without subscribers are both publishes.
	const payload = "hello" // 5 bytes
	for i := 0; i < 3; i++ {
		if err := nc.Publish("a.b", []byte(payload)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := nc.Publish("c.d", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	v := pubView(s.pubStats.Load(), DEFAULT_GLOBAL_ACCOUNT)
	if v == nil {
		t.Fatal("expected ledger row for the default global account")
	}
	rows := map[string]PubStatsSubject{}
	for _, row := range v.Subjects {
		rows[row.Subject] = row
	}
	if r := rows["a.b"]; r.Msgs != 3 || r.Bytes != 15 {
		t.Fatalf("a.b counters wrong: %+v", r)
	}
	if r := rows["c.d"]; r.Msgs != 1 || r.Bytes != 1 {
		t.Fatalf("c.d counters wrong: %+v", r)
	}
	if msgs, bytes := pubTotals(v); msgs != 4 || bytes != 16 {
		t.Fatalf("ledger totals wrong: %d/%d", msgs, bytes)
	}

	// Existing aggregate stats remain intact alongside the ledger.
	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if in := atomic.LoadInt64(&s.inMsgs); in != 4 {
			return fmt.Errorf("inMsgs = %d, want 4", in)
		}
		return nil
	})
}

// Internal clients (SYSTEM/JETSTREAM/ACCOUNT, including MQTT retained
// redelivery) re-enter processInboundClientMsg but must not be counted.
func TestServerPublishStatsSkipsInternalClient(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.PublishStats.Enabled = true
	s := RunServer(&opts)
	defer s.Shutdown()

	ic := s.createInternalAccountClient()
	if ic == nil {
		t.Fatal("expected an internal account client")
	}
	ic.mu.Lock()
	ic.acc = s.gacc
	ic.pa.subject = []byte("internal.x")
	ic.mu.Unlock()
	if _, permIssue := ic.processInboundClientMsg([]byte("payload\r\n")); permIssue {
		t.Fatal("unexpected permission issue")
	}

	if v := pubView(s.pubStats.Load(), DEFAULT_GLOBAL_ACCOUNT); v != nil {
		t.Fatalf("internal-client publish must not be counted, got %+v", v)
	}
}

// When disabled, no manager exists and normal publishing still works.
func TestServerPublishStatsDisabled(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	s := RunServer(&opts)
	defer s.Shutdown()

	if s.pubStats.Load() != nil {
		t.Fatal("ledger manager must be nil when disabled")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	if err := nc.Publish("a.b", []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if in := atomic.LoadInt64(&s.inMsgs); in != 1 {
			return fmt.Errorf("inMsgs = %d, want 1", in)
		}
		return nil
	})
}

// --- HTTP /pubstatz endpoint ---

func pubstatzURL(s *Server, query string) string {
	u := fmt.Sprintf("http://127.0.0.1:%d%s", s.MonitorAddr().Port, PubStatszPath)
	if query != "" {
		u += "?" + query
	}
	return u
}

func getPubStatsz(url string) (*PubStatsz, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("expected 200 from %s, got %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var z PubStatsz
	if err := json.Unmarshal(body, &z); err != nil {
		return nil, err
	}
	return &z, nil
}

func fetchPubStatsz(t testing.TB, url string) *PubStatsz {
	t.Helper()
	z, err := getPubStatsz(url)
	if err != nil {
		t.Fatalf("fetch %s: %v", url, err)
	}
	return z
}

// Disabled server reports enabled:false; reset has no effect.
func TestPubStatszHTTPDisabled(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	s := RunServer(opts)
	defer s.Shutdown()

	z := fetchPubStatsz(t, pubstatzURL(s, ""))
	if z.Enabled || len(z.Accounts) != 0 || z.ResetTime != nil || z.MaxSubjectsPerAccount != 0 {
		t.Fatalf("unexpected disabled response: %+v", z)
	}

	z = fetchPubStatsz(t, pubstatzURL(s, "reset=true"))
	if z.Enabled || z.ResetTime != nil {
		t.Fatalf("reset must not take effect while disabled: %+v", z)
	}
}

// Full path: counters, accounts filter with zero view, reset and old-reader
// forward compatibility.
func TestPubStatszHTTPCountersFilterReset(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.PublishStats.Enabled = true
	opts.PublishStats.MaxSubjectsPerAccount = 32
	aA := NewAccount("A")
	aB := NewAccount("B")
	opts.Accounts = append(opts.Accounts, aA, aB)
	opts.Users = append(opts.Users,
		&User{Username: "a", Password: "a", Account: aA},
		&User{Username: "b", Password: "b", Account: aB})
	s := RunServer(opts)
	defer s.Shutdown()

	ncA, err := nats.Connect(s.ClientURL(), nats.UserInfo("a", "a"))
	if err != nil {
		t.Fatalf("connect A: %v", err)
	}
	defer ncA.Close()
	for i := 0; i < 5; i++ {
		if err := ncA.Publish("a.hot", []byte("12")); err != nil { // 2 bytes
			t.Fatalf("publish: %v", err)
		}
	}
	if err := ncA.Publish("a.once", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := ncA.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Unfiltered snapshot.
	z := fetchPubStatsz(t, pubstatzURL(s, ""))
	if !z.Enabled || z.MaxSubjectsPerAccount != 32 {
		t.Fatalf("bad header: %+v", z)
	}
	byName := map[string]*PubStatsAccount{}
	for _, a := range z.Accounts {
		byName[a.Account] = a
	}
	a := byName["A"]
	if a == nil {
		t.Fatal("missing account A")
	}
	rows := map[string]PubStatsSubject{}
	for _, r := range a.Subjects {
		rows[r.Subject] = r
	}
	if r := rows["a.hot"]; r.Msgs != 5 || r.Bytes != 10 {
		t.Fatalf("a.hot wrong: %+v", r)
	}
	if r := rows["a.once"]; r.Msgs != 1 || r.Bytes != 1 {
		t.Fatalf("a.once wrong: %+v", r)
	}
	if a.Overflow || a.EvictedSubjects != 0 || a.Other.Msgs != 0 {
		t.Fatalf("unexpected overflow state: %+v", a)
	}
	if _, present := byName["B"]; present {
		t.Fatal("quiet account should not appear without filter")
	}

	// Filter: A rows + zero view for existing-but-quiet B; unknown ignored.
	z = fetchPubStatsz(t, pubstatzURL(s, "accounts=A,B,NOPE"))
	byName = map[string]*PubStatsAccount{}
	for _, acc := range z.Accounts {
		byName[acc.Account] = acc
	}
	if len(z.Accounts) != 2 {
		t.Fatalf("expected 2 accounts, got %d", len(z.Accounts))
	}
	if q := byName["B"]; q == nil || len(q.Subjects) != 0 || q.Overflow || q.EvictedSubjects != 0 {
		t.Fatalf("zero view for B wrong: %+v", q)
	}
	// Zero view must not have materialized a ledger.
	if _, ok := s.pubStats.Load().ledgers.Load(aB); ok {
		t.Fatal("filtered snapshot must not create a ledger for B")
	}

	// Global reset returns the post-reset zero snapshot with reset_time.
	z = fetchPubStatsz(t, pubstatzURL(s, "accounts=A&reset=true"))
	a = nil
	for _, acc := range z.Accounts {
		if acc.Account == "A" {
			a = acc
		}
	}
	if a == nil || len(a.Subjects) != 0 || a.Overflow || a.EvictedSubjects != 0 || a.Other.Msgs != 0 || a.Other.Bytes != 0 {
		t.Fatalf("post-reset A view wrong: %+v", a)
	}
	if z.ResetTime == nil {
		t.Fatal("reset response should carry reset_time")
	}

	// Subsequent ordinary reads stay zero; counters then accumulate anew.
	z = fetchPubStatsz(t, pubstatzURL(s, "accounts=A"))
	if len(z.Accounts[0].Subjects) != 0 || z.ResetTime == nil {
		t.Fatalf("post-reset read should stay zero and retain reset_time: %+v", z)
	}
	if err := ncA.Publish("a.hot", []byte("z")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := ncA.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		z = fetchPubStatsz(t, pubstatzURL(s, "accounts=A"))
		if len(z.Accounts[0].Subjects) != 1 || z.Accounts[0].Subjects[0].Msgs != 1 {
			return fmt.Errorf("expected a single fresh row after reset, got %+v", z.Accounts[0])
		}
		return nil
	})

	// Forward compatibility: an "old" reader struct that only knows a few
	// fields still decodes the response without error.
	resp, err := http.Get(pubstatzURL(s, ""))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	oldStyle := struct {
		ServerID string `json:"server_id"`
		Enabled  bool   `json:"enabled"`
	}{}
	if err := json.NewDecoder(resp.Body).Decode(&oldStyle); err != nil {
		t.Fatalf("old reader should ignore new fields: %v", err)
	}
	if !oldStyle.Enabled || oldStyle.ServerID != s.ID() {
		t.Fatalf("old reader decode wrong: %+v", oldStyle)
	}
}

// Memory stays bounded as subject cardinality grows far beyond capacity, and
// counters never lose totals during continuous eviction.
func TestPubSubjectLedgerMemoryBound(t *testing.T) {
	mgr := newPubStatsMgr(32)
	acc := &Account{Name: "acc"}

	burst := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			mgr.record(acc, fmt.Sprintf("%s.%06d", prefix, i), 10)
		}
	}

	burst("a", 100000)
	runtime.GC()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	burst("b", 100000)
	runtime.GC()
	runtime.ReadMemStats(&m2)

	v := pubView(mgr, "acc")
	if v.RetainedSubjects > 32 {
		t.Fatalf("retained %d exceeds capacity", v.RetainedSubjects)
	}
	if !v.Overflow || v.EvictedSubjects == 0 {
		t.Fatalf("expected overflow markers: %+v", v)
	}
	if msgs, _ := pubTotals(v); msgs != 200000 {
		t.Fatalf("counters lost during churn: got %d want 200000", msgs)
	}
	// An unbounded implementation would grow roughly with the extra 100k
	// entries (several MB). The bounded ledger churns within its capacity.
	if growth := int64(m2.HeapAlloc) - int64(m1.HeapAlloc); growth > 4<<20 {
		t.Fatalf("heap grew by %d bytes across the second 100k-subject burst; ledger appears unbounded", growth)
	}
}
