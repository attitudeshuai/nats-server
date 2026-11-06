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
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Accepted publishes are counted under the post-mapping subject; permission
// denied publishes are not counted; no-interest publishes are counted.
func TestPubStatsE2EMappingAndDeny(t *testing.T) {
	cf := createConfFile(t, []byte(`
port: -1
publish_stats: { enabled: true, max_subjects_per_account: 64 }
authorization {
  users = [
    { user: "u", password: "p",
      permissions { publish { deny = ["deny.>"] } } }
  ]
}
mappings = {
  "map.foo": map.bar
}
`))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("u", "p"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	// Accepted plain publish (no subscriber): counted.
	if err := nc.Publish("plain", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Mapped publish: counted under the destination subject only.
	if err := nc.Publish("map.foo", []byte("mm")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Denied publish: dropped by the server, must never reach the ledger.
	denied, err := nats.Connect(s.ClientURL(), nats.UserInfo("u", "p"))
	if err != nil {
		t.Fatalf("connect denied conn: %v", err)
	}
	if err := denied.Publish("deny.x", []byte("nope")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	denied.Flush()
	denied.Close()

	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	checkFor(t, time.Second, 5*time.Millisecond, func() error {
		v := pubView(s.pubStats.Load(), DEFAULT_GLOBAL_ACCOUNT)
		if v == nil {
			return fmt.Errorf("no ledger view yet")
		}
		rows := map[string]PubStatsSubject{}
		for _, r := range v.Subjects {
			rows[r.Subject] = r
		}
		if _, ok := rows["deny.x"]; ok {
			return fmt.Errorf("denied publish was counted")
		}
		if _, ok := rows["map.foo"]; ok {
			return fmt.Errorf("mapped source subject must not be counted")
		}
		if r := rows["map.bar"]; r.Msgs != 1 || r.Bytes != 2 {
			return fmt.Errorf("mapped dest wrong: %+v", r)
		}
		if r := rows["plain"]; r.Msgs != 1 || r.Bytes != 5 {
			return fmt.Errorf("plain wrong: %+v", r)
		}
		msgs, bytes := pubTotals(v)
		if msgs != 2 || bytes != 7 {
			return fmt.Errorf("total wrong: %d/%d", msgs, bytes)
		}
		return nil
	})
}

// Reload semantics: unchanged config preserves; toggle/capacity change resets
// to a fresh zero-state manager.
func TestPubStatsE2EReload(t *testing.T) {
	cf := createConfFile(t, []byte(`
port: -1
publish_stats: { enabled: true, max_subjects_per_account: 16 }
`))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	if err := nc.Publish("a.b", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	nc.Flush()
	checkFor(t, time.Second, 5*time.Millisecond, func() error {
		v := pubView(s.pubStats.Load(), DEFAULT_GLOBAL_ACCOUNT)
		if v == nil || len(v.Subjects) != 1 {
			return fmt.Errorf("waiting for initial count: %+v", v)
		}
		return nil
	})
	mgrBefore := s.pubStats.Load()

	rewrite := func(body string) {
		t.Helper()
		if err := os.WriteFile(cf, []byte(body), 0644); err != nil {
			t.Fatalf("rewrite config: %v", err)
		}
		if err := s.Reload(); err != nil {
			t.Fatalf("reload: %v", err)
		}
	}

	// 1) Same effective config: preserved.
	rewrite("port: -1\npublish_stats: { enabled: true, max_subjects_per_account: 16 }\n")
	if s.pubStats.Load() != mgrBefore {
		t.Fatal("unchanged publish_stats config must preserve the manager")
	}
	if v := pubView(s.pubStats.Load(), DEFAULT_GLOBAL_ACCOUNT); v == nil || len(v.Subjects) != 1 {
		t.Fatalf("counts should survive an unrelated reload: %+v", v)
	}

	// 2) Capacity change: fresh zero-state manager with the new capacity.
	rewrite("port: -1\npublish_stats: { enabled: true, max_subjects_per_account: 8 }\n")
	m := s.pubStats.Load()
	if m == nil || m == mgrBefore || m.capacity != 8 {
		t.Fatalf("capacity change must install a fresh manager: %+v", m)
	}
	if v := pubView(m, DEFAULT_GLOBAL_ACCOUNT); v != nil {
		t.Fatalf("new manager must start empty, got %+v", v)
	}

	// 3) Disable: manager gone; publishing keeps working on aggregate stats.
	rewrite("port: -1\npublish_stats: { enabled: false }\n")
	if s.pubStats.Load() != nil {
		t.Fatal("manager must be removed on disable")
	}
	if err := nc.Publish("c.d", []byte("yy")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	nc.Flush()

	// 4) Re-enable: fresh zero-state manager.
	rewrite("port: -1\npublish_stats: { enabled: true }\n")
	if m = s.pubStats.Load(); m == nil || m.capacity != defaultPublishStatsMaxSubjects {
		t.Fatalf("re-enable must install a fresh default-capacity manager: %+v", m)
	}

	// 5) A failed reload (bad config) must not touch the running manager.
	good := "port: -1\npublish_stats: { enabled: true }\n"
	if err := os.WriteFile(cf, []byte("port: -1\npublish_stats: { enabled: true, max_subjects_per_account: -1 }\n"), 0644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := s.Reload(); err == nil {
		t.Fatal("expected invalid reload to fail")
	}
	if s.pubStats.Load() != m {
		t.Fatal("failed reload must not replace the manager")
	}
	if err := os.WriteFile(cf, []byte(good), 0644); err != nil {
		t.Fatalf("restore config: %v", err)
	}
}

// Deleting an account through reload drops its ledger; a recreated account of
// the same name starts from zero.
func TestPubStatsE2EReloadAccountDeletion(t *testing.T) {
	cf := createConfFile(t, []byte(`
port: -1
accounts {
  A { users = [ { user: "a", password: "a" } ] }
}
publish_stats: { enabled: true }
`))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("a", "a"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := nc.Publish("a.keep", []byte("1")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	checkFor(t, time.Second, 5*time.Millisecond, func() error {
		v := pubView(s.pubStats.Load(), "A")
		if v == nil || len(v.Subjects) != 1 {
			return fmt.Errorf("waiting initial A counts: %+v", v)
		}
		return nil
	})
	nc.Close()

	// Remove account A.
	if err := os.WriteFile(cf, []byte("port: -1\npublish_stats: { enabled: true }\n"), 0644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("reload delete: %v", err)
	}
	checkFor(t, time.Second, 5*time.Millisecond, func() error {
		if _, ok := s.accounts.Load("A"); ok {
			return fmt.Errorf("account A still registered")
		}
		return nil
	})
	if v := pubView(s.pubStats.Load(), "A"); v != nil {
		t.Fatalf("ledger for deleted account must be gone: %+v", v)
	}

	// Recreate account A with the same name and publish again.
	if err := os.WriteFile(cf, []byte(`
port: -1
accounts {
  A { users = [ { user: "a", password: "a" } ] }
}
publish_stats: { enabled: true }
`), 0644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("reload recreate: %v", err)
	}
	nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("a", "a"))
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer nc2.Close()
	if err := nc2.Publish("a.fresh", []byte("z")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	nc2.Flush()
	checkFor(t, time.Second, 5*time.Millisecond, func() error {
		v := pubView(s.pubStats.Load(), "A")
		if v == nil {
			return fmt.Errorf("no view for recreated A")
		}
		rows := map[string]PubStatsSubject{}
		for _, r := range v.Subjects {
			rows[r.Subject] = r
		}
		if _, ok := rows["a.keep"]; ok {
			return fmt.Errorf("previous-round counts leaked into recreated account")
		}
		if r := rows["a.fresh"]; r.Msgs != 1 || r.Bytes != 1 {
			return fmt.Errorf("recreated account counts wrong: %+v", r)
		}
		if msgs, _ := pubTotals(v); msgs != 1 {
			return fmt.Errorf("recreated account must start from zero, total=%d", msgs)
		}
		return nil
	})
}

// A publish arriving through a leafnode is counted at the hub's ledger
// (LEAF ingress); the leaf-side server also counts it as CLIENT ingress.
func TestPubStatsE2ELeafIngress(t *testing.T) {
	o1 := DefaultOptions()
	o1.Port = -1
	o1.LeafNode.Host = "127.0.0.1"
	o1.LeafNode.Port = -1
	o1.PublishStats.Enabled = true
	o1.PublishStats.MaxSubjectsPerAccount = 64
	hub := RunServer(o1)
	defer hub.Shutdown()

	o2 := DefaultOptions()
	o2.Port = -1
	o2.Cluster.Name = "leaf-test"
	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port))
	o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	o2.PublishStats.Enabled = true
	o2.PublishStats.MaxSubjectsPerAccount = 64
	leaf := RunServer(o2)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	// Leaf links are interest-driven: a hub-side subscriber makes the leaf
	// forward the publishes to the hub.
	hc := natsConnect(t, hub.ClientURL())
	defer hc.Close()
	if _, err := hc.SubscribeSync("leaf.x"); err != nil {
		t.Fatalf("hub subscribe: %v", err)
	}
	hc.Flush()
	time.Sleep(50 * time.Millisecond)

	nc := natsConnect(t, leaf.ClientURL())
	defer nc.Close()
	for i := 0; i < 7; i++ {
		if err := nc.Publish("leaf.x", []byte("abc")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		vHub := pubView(hub.pubStats.Load(), DEFAULT_GLOBAL_ACCOUNT)
		if vHub == nil {
			return fmt.Errorf("hub ledger missing")
		}
		var row *PubStatsSubject
		for i := range vHub.Subjects {
			if vHub.Subjects[i].Subject == "leaf.x" {
				row = &vHub.Subjects[i]
			}
		}
		if row == nil || row.Msgs != 7 || row.Bytes != 21 {
			return fmt.Errorf("hub LEAF ingress wrong: %+v", row)
		}
		vLeaf := pubView(leaf.pubStats.Load(), DEFAULT_GLOBAL_ACCOUNT)
		if vLeaf == nil {
			return fmt.Errorf("leaf ledger missing")
		}
		msgs, bytes := pubTotals(vLeaf)
		if msgs != 7 || bytes != 21 {
			return fmt.Errorf("leaf CLIENT ingress wrong: %d/%d", msgs, bytes)
		}
		return nil
	})
}

// Past capacity the overflow marker, aggregate bucket and retained hot
// subjects are observable over HTTP; retained size stays bounded.
func TestPubStatsE2EHTTPOverflow(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.PublishStats.Enabled = true
	opts.PublishStats.MaxSubjectsPerAccount = 16
	s := RunServer(opts)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	const unique = 500
	for i := 0; i < unique; i++ {
		if err := nc.Publish(fmt.Sprintf("cold.%04d", i), []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	for i := 0; i < 200; i++ {
		if err := nc.Publish("hot.keep", []byte("yy")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	z := fetchPubStatsz(t, pubstatzURL(s, ""))
	if len(z.Accounts) != 1 {
		t.Fatalf("expected one account, got %d", len(z.Accounts))
	}
	a := z.Accounts[0]
	if a.RetainedSubjects > 16 {
		t.Fatalf("retained %d exceeds capacity", a.RetainedSubjects)
	}
	if !a.Overflow || a.EvictedSubjects == 0 || a.Other.Msgs == 0 {
		t.Fatalf("overflow state missing: %+v", a)
	}
	var hot *PubStatsSubject
	for i := range a.Subjects {
		if a.Subjects[i].Subject == "hot.keep" {
			hot = &a.Subjects[i]
		}
	}
	if hot == nil || hot.Msgs != 200 || hot.Bytes != 400 {
		t.Fatalf("hot subject should be retained exactly: %+v", hot)
	}
	if msgs, _ := pubTotals(a); msgs != unique+200 {
		t.Fatalf("overflow ledger total wrong: %d want %d", msgs, unique+200)
	}
}

// Continuous publishing, concurrent snapshot reads and concurrent resets over
// HTTP: every reset response is an all-account zero state, every snapshot is
// structurally valid, and after a final reset a controlled burst counts
// exactly.
func TestPubStatsE2ESnapshotAndResetConcurrent(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.PublishStats.Enabled = true
	opts.PublishStats.MaxSubjectsPerAccount = 64
	s := RunServer(opts)
	defer s.Shutdown()

	const (
		publishers = 4
		subjectsN  = 8
	)
	var pwg sync.WaitGroup
	pubStop := make(chan struct{})
	for p := 0; p < publishers; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()
			for i := 0; ; i++ {
				select {
				case <-pubStop:
					nc.Flush()
					return
				default:
				}
				if err := nc.Publish(fmt.Sprintf("s.%d", (p+i)%subjectsN), []byte{byte('a' + i%26)}); err != nil {
					return
				}
				if i%256 == 0 {
					nc.Flush()
				}
			}
		}(p)
	}

	// Readers: structural validity of every cross-section, including during
	// resets.
	var rwg sync.WaitGroup
	for r := 0; r < 2; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for i := 0; i < 100; i++ {
				z, err := getPubStatsz(pubstatzURL(s, ""))
				if err != nil {
					t.Errorf("reader fetch: %v", err)
					return
				}
				seen := map[string]struct{}{}
				for _, a := range z.Accounts {
					if a.RetainedSubjects > 64 {
						t.Errorf("retained exceeds capacity")
						return
					}
					if a.RetainedSubjects != len(a.Subjects) {
						t.Errorf("retained mismatch")
						return
					}
					for _, row := range a.Subjects {
						if _, dup := seen[a.Account+"/"+row.Subject]; dup {
							t.Errorf("duplicate subject %s", row.Subject)
							return
						}
						seen[a.Account+"/"+row.Subject] = struct{}{}
						if row.Msgs <= 0 || row.Bytes <= 0 {
							t.Errorf("non-positive row: %+v", row)
							return
						}
					}
				}
			}
		}()
	}

	// Resetters: every response must be fully zero across every account.
	for r := 0; r < 10; r++ {
		z := fetchPubStatsz(t, pubstatzURL(s, "reset=true"))
		if z.ResetTime == nil {
			t.Fatal("reset response missing reset_time")
		}
		for _, a := range z.Accounts {
			if len(a.Subjects) != 0 || a.Overflow || a.EvictedSubjects != 0 || a.Other.Msgs != 0 || a.Other.Bytes != 0 {
				t.Fatalf("reset response must be zero for all accounts, got %+v", a)
			}
		}
		time.Sleep(2 * time.Millisecond)
	}

	close(pubStop)
	pwg.Wait()
	rwg.Wait()

	// Final reset, then a controlled quiescent burst must count exactly.
	fetchPubStatsz(t, pubstatzURL(s, "reset=true"))
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	for i := 0; i < 100; i++ {
		if err := nc.Publish("exact.one", []byte("qq")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		a := fetchPubStatsz(t, pubstatzURL(s, "")).Accounts[0]
		if len(a.Subjects) != 1 || a.Subjects[0].Msgs != 100 || a.Subjects[0].Bytes != 200 {
			return fmt.Errorf("post-reset burst mismatch: %+v", a)
		}
		if msgs, _ := pubTotals(a); msgs != 100 {
			return fmt.Errorf("total %d", msgs)
		}
		return nil
	})
}

// Disabled server: existing endpoints carry no publish_stats fields and keep
// reporting the aggregate counters; the new endpoint simply says disabled.
func TestPubStatsE2EDisabledContract(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	s := RunServer(opts)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	if err := nc.Publish("a.b", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	nc.Flush()

	for _, path := range []string{VarzPath, AccountStatzPath} {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", s.MonitorAddr().Port, path))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		var doc map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		resp.Body.Close()
		var walk func(prefix string, v any)
		walk = func(prefix string, v any) {
			switch vv := v.(type) {
			case map[string]any:
				for k, v := range vv {
					if strings.Contains(k, "publish") {
						t.Errorf("%s leaks publish_stats field %q", path, k)
					}
					walk(prefix+"/"+k, v)
				}
			case []any:
				for _, v := range vv {
					walk(prefix, v)
				}
			}
		}
		walk("", doc)
		switch path {
		case VarzPath:
			if in, ok := doc["in_msgs"]; !ok || in.(float64) < 1 {
				t.Errorf("%s missing in_msgs aggregate counter", path)
			}
		case AccountStatzPath:
			stz, ok := doc["account_statz"].([]any)
			if !ok || len(stz) == 0 {
				t.Errorf("%s missing account_statz aggregate counters", path)
			}
		}
	}

	z := fetchPubStatsz(t, pubstatzURL(s, ""))
	if z.Enabled || len(z.Accounts) != 0 || z.ResetTime != nil {
		t.Fatalf("new endpoint must report disabled only: %+v", z)
	}
}
