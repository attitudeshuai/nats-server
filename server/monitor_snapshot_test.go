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
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func snapshotAllViews(t *testing.T, s *Server, opts *SnapshotzOptions) *Snapshotz {
	t.Helper()
	if opts == nil {
		opts = &SnapshotzOptions{}
	}
	z, err := s.Snapshotz(opts)
	if err != nil {
		t.Fatalf("Snapshotz failed: %v", err)
	}
	if z == nil {
		t.Fatalf("Snapshotz returned nil snapshot")
	}
	return z
}

// TestSnapshotzConsistentAcrossViews verifies the core guarantee: all views
// of one snapshot share the same logical instant and an entity cannot be
// present in one view but missing from another.
func TestSnapshotzConsistentAcrossViews(t *testing.T) {
	s := runMonitorServerWithAccounts()
	defer s.Shutdown()

	ncA1 := createClientConnWithUserSubscribeAndPublish(t, s, "a", "a")
	defer ncA1.Close()
	ncA2 := createClientConnWithUserSubscribeAndPublish(t, s, "a", "a")
	defer ncA2.Close()
	ncB := createClientConnWithUserSubscribeAndPublish(t, s, "b", "b")
	defer ncB.Close()

	mustSub(t, ncA1, "foo.*")
	mustSub(t, ncA1, "bar.>")
	mustSub(t, ncA2, "foo.bar")
	mustSub(t, ncB, "baz")
	time.Sleep(50 * time.Millisecond)

	z := snapshotAllViews(t, s, &SnapshotzOptions{
		Connz:  &ConnzOptions{Subscriptions: true},
		Subsz:  &SubszOptions{Subscriptions: true},
		Routez: &RoutezOptions{},
	})

	if z.ID == _EMPTY_ {
		t.Fatalf("Snapshot id should be set")
	}
	if len(z.Views) != 4 {
		t.Fatalf("Expected 4 default views, got %v", z.Views)
	}
	if z.Connz == nil || z.Subsz == nil || z.Routez == nil || z.Accountz == nil {
		t.Fatalf("All four views should be present: %+v", z)
	}

	// All views must carry the same timestamp and server id.
	if !z.Connz.Now.Equal(z.Now) || !z.Subsz.Now.Equal(z.Now) ||
		!z.Routez.Now.Equal(z.Now) || !z.Accountz.Now.Equal(z.Now) {
		t.Fatalf("Views do not share the snapshot timestamp: %v %v %v %v vs %v",
			z.Connz.Now, z.Subsz.Now, z.Routez.Now, z.Accountz.Now, z.Now)
	}
	if z.Connz.ID != z.ServerID || z.Subsz.ID != z.ServerID ||
		z.Routez.ID != z.ServerID || z.Accountz.ID != z.ServerID {
		t.Fatalf("Views do not share the server id")
	}

	// Connection view: 3 open clients.
	if z.Connz.NumConns != 3 || z.Connz.Total != 3 {
		t.Fatalf("Expected 3 connections, got num=%d total=%d", z.Connz.NumConns, z.Connz.Total)
	}
	cidSet := make(map[uint64]struct{}, 3)
	subsByCid := make(map[uint64]int)
	for _, ci := range z.Connz.Conns {
		cidSet[ci.Cid] = struct{}{}
		subsByCid[ci.Cid] = int(ci.NumSubs)
	}

	// Route view: no routes in this server setup.
	ridSet := make(map[uint64]struct{})
	for _, ri := range z.Routez.Routes {
		ridSet[ri.Rid] = struct{}{}
	}

	// Every subscription detail must reference a connection present in the
	// connection or route view of THIS snapshot.
	if z.Subsz.Total != 4 {
		t.Fatalf("Expected 4 subscription details, got %d", z.Subsz.Total)
	}
	subDetailByCid := make(map[uint64]int)
	for _, sd := range z.Subsz.Subs {
		subDetailByCid[sd.Cid]++
		if _, ok := cidSet[sd.Cid]; !ok {
			if _, ok := ridSet[sd.Cid]; !ok {
				t.Fatalf("Subscription cid %d not present in connz or routez views", sd.Cid)
			}
		}
	}
	// Connection subscription counts must add up with the subscription view.
	if !reflect.DeepEqual(subsByCid, subDetailByCid) {
		t.Fatalf("ConnInfo subscription counts %v do not match subsz details %v", subsByCid, subDetailByCid)
	}

	// Account list contains the two declared accounts.
	accs := make(map[string]struct{})
	for _, name := range z.Accountz.Accounts {
		accs[name] = struct{}{}
	}
	if _, ok := accs["A"]; !ok {
		t.Fatalf("Account A missing from accountz view: %v", z.Accountz.Accounts)
	}
	if _, ok := accs["B"]; !ok {
		t.Fatalf("Account B missing from accountz view: %v", z.Accountz.Accounts)
	}

	// Account-scoped snapshot: accountz client count must equal the number of
	// connections the connz view returns for the same account.
	za := snapshotAllViews(t, s, &SnapshotzOptions{
		Views:    []string{SnapshotViewConnz, SnapshotViewAccountz},
		Connz:    &ConnzOptions{Account: "A"},
		Accountz: &AccountzOptions{Account: "A"},
	})
	if za.Connz.NumConns != za.Accountz.Account.ClientCnt+za.Accountz.Account.LeafCnt {
		t.Fatalf("Account A connz count %d != accountz client %d + leaf %d",
			za.Connz.NumConns, za.Accountz.Account.ClientCnt, za.Accountz.Account.LeafCnt)
	}
	if za.Accountz.Account.ClientCnt != 2 || za.Accountz.Account.LeafCnt != 0 {
		t.Fatalf("Expected 2 clients and 0 leafs for A, got %d/%d",
			za.Accountz.Account.ClientCnt, za.Accountz.Account.LeafCnt)
	}
	if za.Accountz.Account.SubCnt != uint32(3) {
		t.Fatalf("Expected 3 subscriptions on A, got %d", za.Accountz.Account.SubCnt)
	}

	// Budget accounting: 3 conns + 4 subs + 0 routes + N accounts, no cuts.
	if z.Budget.Truncated || z.Budget.LimitedBy != _EMPTY_ {
		t.Fatalf("Did not expect truncation: %+v", z.Budget)
	}
	if z.Budget.ItemsReturned != 3+4+0+len(z.Accountz.Accounts) {
		t.Fatalf("Unexpected items returned: %d", z.Budget.ItemsReturned)
	}
	if z.Budget.BytesReturned <= 0 {
		t.Fatalf("Expected positive bytes returned")
	}
	if z.Budget.MaxItems != DefaultSnapshotMaxItems || z.Budget.MaxBytes != DefaultSnapshotMaxBytes {
		t.Fatalf("Budget defaults not applied: %+v", z.Budget)
	}
	if z.Connz.SnapshotTrunc != nil || z.Subsz.SnapshotTrunc != nil ||
		z.Routez.SnapshotTrunc != nil || z.Accountz.SnapshotTrunc != nil {
		t.Fatalf("No view should carry a truncation marker")
	}
}

// TestSnapshotzMaxItemsTruncation verifies the shared item ceiling.
func TestSnapshotzMaxItemsTruncation(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	var ncs []*nats.Conn
	for i := 0; i < 5; i++ {
		nc := createClientConnSubscribeAndPublish(t, s)
		ncs = append(ncs, nc)
	}
	defer func() {
		for _, nc := range ncs {
			nc.Close()
		}
	}()

	standaloneAccounts, err := s.Accountz(nil)
	if err != nil {
		t.Fatalf("standalone accountz: %v", err)
	}
	nAccounts := len(standaloneAccounts.Accounts)

	z := snapshotAllViews(t, s, &SnapshotzOptions{
		Views:    []string{SnapshotViewConnz, SnapshotViewAccountz},
		MaxItems: 3,
	})

	if !z.Budget.Truncated || z.Budget.LimitedBy != snapshotLimitedByItems {
		t.Fatalf("Expected max_items truncation, got %+v", z.Budget)
	}
	if z.Budget.ItemsReturned != 3 {
		t.Fatalf("Expected 3 items returned, got %d", z.Budget.ItemsReturned)
	}
	if z.Budget.ItemsReturned > z.Budget.MaxItems {
		t.Fatalf("Returned items exceed declared ceiling")
	}

	if z.Connz.NumConns != 3 || z.Connz.Total != 5 {
		t.Fatalf("Expected 3 of 5 conns, got num=%d total=%d", z.Connz.NumConns, z.Connz.Total)
	}
	if z.Connz.SnapshotTrunc == nil {
		t.Fatalf("connz should be marked truncated")
	}
	if z.Connz.SnapshotTrunc.Reason != snapshotLimitedByItems ||
		z.Connz.SnapshotTrunc.Returned != 3 ||
		z.Connz.SnapshotTrunc.Available != 5 ||
		z.Connz.SnapshotTrunc.Dropped != 2 {
		t.Fatalf("Bad connz truncation marker: %+v", z.Connz.SnapshotTrunc)
	}

	// The accountz view comes after connz and gets no budget left.
	if z.Accountz == nil || z.Accountz.SnapshotTrunc == nil {
		t.Fatalf("accountz should be present and marked truncated")
	}
	if z.Accountz.SnapshotTrunc.Returned != 0 ||
		z.Accountz.SnapshotTrunc.Available != nAccounts ||
		z.Accountz.SnapshotTrunc.Dropped != nAccounts ||
		z.Accountz.SnapshotTrunc.Reason != snapshotLimitedByItems {
		t.Fatalf("Bad accountz truncation marker: %+v (want available %d)",
			z.Accountz.SnapshotTrunc, nAccounts)
	}
	if len(z.Accountz.Accounts) != 0 {
		t.Fatalf("Truncated accountz should contain no entries, got %v", z.Accountz.Accounts)
	}
}

// TestSnapshotzMaxBytesTruncation verifies the shared byte ceiling.
func TestSnapshotzMaxBytesTruncation(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	var ncs []*nats.Conn
	for i := 0; i < 3; i++ {
		ncs = append(ncs, createClientConnSubscribeAndPublish(t, s))
	}
	defer func() {
		for _, nc := range ncs {
			nc.Close()
		}
	}()

	// Size the budget to exactly fit one connection entry (sorted by cid).
	standalone := snapshotAllViews(t, s, &SnapshotzOptions{Views: []string{SnapshotViewConnz}})
	first, err := json.Marshal(standalone.Connz.Conns[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	z := snapshotAllViews(t, s, &SnapshotzOptions{
		Views:    []string{SnapshotViewConnz, SnapshotViewAccountz},
		MaxBytes: len(first),
	})
	if !z.Budget.Truncated || z.Budget.LimitedBy != snapshotLimitedByBytes {
		t.Fatalf("Expected max_bytes truncation, got %+v", z.Budget)
	}
	if z.Connz.NumConns != 1 {
		t.Fatalf("Expected exactly 1 conn within byte budget, got %d", z.Connz.NumConns)
	}
	if z.Budget.BytesReturned != len(first) {
		t.Fatalf("Expected %d bytes returned, got %d", len(first), z.Budget.BytesReturned)
	}
	if z.Budget.BytesReturned > z.Budget.MaxBytes {
		t.Fatalf("Returned bytes exceed declared ceiling")
	}
	if z.Connz.SnapshotTrunc == nil || z.Connz.SnapshotTrunc.Reason != snapshotLimitedByBytes ||
		z.Connz.SnapshotTrunc.Available != 3 || z.Connz.SnapshotTrunc.Returned != 1 {
		t.Fatalf("Bad connz byte truncation: %+v", z.Connz.SnapshotTrunc)
	}
	if z.Accountz.SnapshotTrunc == nil || z.Accountz.SnapshotTrunc.Returned != 0 {
		t.Fatalf("accountz should be fully cut, got %+v", z.Accountz.SnapshotTrunc)
	}

	// A budget smaller than any entry yields zero entries, never a panic.
	z0 := snapshotAllViews(t, s, &SnapshotzOptions{
		Views:    []string{SnapshotViewConnz},
		MaxBytes: 1,
	})
	if z0.Connz.NumConns != 0 || z0.Budget.BytesReturned != 0 {
		t.Fatalf("Expected empty connz under 1-byte budget, got %+v", z0.Connz)
	}
	if z0.Connz.SnapshotTrunc == nil || z0.Connz.SnapshotTrunc.Reason != snapshotLimitedByBytes {
		t.Fatalf("Expected byte truncation marker, got %+v", z0.Connz.SnapshotTrunc)
	}
}

// TestSnapshotzViewFailureFailsWhole verifies all-or-nothing semantics and
// that failures do not poison later requests.
func TestSnapshotzViewFailureFailsWhole(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	// An accountz view for a missing account makes the whole collection fail.
	z, err := s.Snapshotz(&SnapshotzOptions{
		Views:    []string{SnapshotViewConnz, SnapshotViewAccountz},
		Accountz: &AccountzOptions{Account: "DOES_NOT_EXIST"},
	})
	if err == nil {
		t.Fatalf("Expected error for unknown account, got %+v", z)
	}
	if z != nil {
		t.Fatalf("No partial snapshot may be published, got %+v", z)
	}

	// Same for an invalid connz sort and an invalid subsz test subject.
	if _, err := s.Snapshotz(&SnapshotzOptions{
		Views: []string{SnapshotViewConnz},
		Connz: &ConnzOptions{Sort: "bogus"},
	}); err == nil {
		t.Fatalf("Expected error for invalid sort")
	}
	if _, err := s.Snapshotz(&SnapshotzOptions{
		Views: []string{SnapshotViewSubsz},
		Subsz: &SubszOptions{Subscriptions: true, Test: "not..valid"},
	}); err == nil {
		t.Fatalf("Expected error for invalid test subject")
	}

	// The failed snapshot id must not be referenceable and later snapshots
	// work normally and get distinct ids.
	z1 := snapshotAllViews(t, s, nil)
	z2 := snapshotAllViews(t, s, nil)
	if z1.ID == _EMPTY_ || z1.ID == z2.ID {
		t.Fatalf("Snapshot ids should be set and unique: %q %q", z1.ID, z2.ID)
	}

	// Standalone endpoints are unaffected.
	if _, err := s.Connz(nil); err != nil {
		t.Fatalf("Standalone connz broken after failures: %v", err)
	}
}

// TestSnapshotzConcurrentIsolation runs many snapshots with different
// budgets concurrently and verifies they never influence each other.
func TestSnapshotzConcurrentIsolation(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	var ncs []*nats.Conn
	for i := 0; i < 5; i++ {
		ncs = append(ncs, createClientConnSubscribeAndPublish(t, s))
	}
	defer func() {
		for _, nc := range ncs {
			nc.Close()
		}
	}()

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := make(map[string]struct{}, n)
	errs := make([]error, 0)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			opts := &SnapshotzOptions{
				Views: []string{SnapshotViewConnz, SnapshotViewAccountz},
			}
			// Even iterations are heavily budgeted; odd ones are not.
			if i%2 == 0 {
				opts.MaxItems = 1
			}
			z, err := s.Snapshotz(opts)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if _, dup := ids[z.ID]; dup {
				errs = append(errs, fmt.Errorf("duplicate snapshot id %q", z.ID))
				return
			}
			ids[z.ID] = struct{}{}
			if i%2 == 0 {
				if !z.Budget.Truncated || z.Budget.ItemsReturned != 1 {
					errs = append(errs, fmt.Errorf("iteration %d: expected small-budget truncation, got %+v", i, z.Budget))
				}
			} else if z.Budget.Truncated {
				errs = append(errs, fmt.Errorf("iteration %d: large-budget snapshot unexpectedly truncated", i))
			}
		}()
	}
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("Concurrent snapshot failures: %v", errs[0])
	}
}

// TestSnapshotzCancelation verifies that canceling a snapshot aborts it and
// leaves no goroutines or server-registered state behind.
func TestSnapshotzCancelation(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	var ncs []*nats.Conn
	for i := 0; i < 3; i++ {
		ncs = append(ncs, createClientConnSubscribeAndPublish(t, s))
	}
	defer func() {
		for _, nc := range ncs {
			nc.Close()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	z, err := s.Snapshotz(&SnapshotzOptions{ctx: ctx})
	if err == nil {
		t.Fatalf("Expected error on canceled context, got %+v", z)
	}
	if z != nil {
		t.Fatalf("No snapshot should be returned after cancellation")
	}

	// The canceled snapshot must not leave goroutines behind and must not
	// affect subsequent snapshots.
	runtime.GC()
	baseline := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		cctx, ccancel := context.WithCancel(context.Background())
		if i%2 == 0 {
			ccancel()
		}
		_, _ = s.Snapshotz(&SnapshotzOptions{ctx: cctx})
		ccancel()
	}
	runtime.GC()
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if n := runtime.NumGoroutine(); n > baseline+2 {
			return fmt.Errorf("goroutine leak: %d vs baseline %d", n, baseline)
		}
		return nil
	})
	z2 := snapshotAllViews(t, s, nil)
	if z2 == nil || z2.Connz == nil || z2.Connz.NumConns != 3 {
		t.Fatalf("Server state poisoned after cancellations: %+v", z2)
	}
}

// TestSnapshotzSingleViewParity verifies that a snapshot containing a single
// view produces exactly the same field values as the standalone call.
func TestSnapshotzSingleViewParity(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc1 := createClientConnSubscribeAndPublish(t, s)
	defer nc1.Close()
	nc2 := createClientConnSubscribeAndPublish(t, s)
	defer nc2.Close()
	mustSub(t, nc1, "foo.bar")
	mustSub(t, nc2, "foo.baz")

	// Connz
	connzOpts := &ConnzOptions{Subscriptions: true}
	wantConnz, err := s.Connz(connzOpts)
	if err != nil {
		t.Fatalf("standalone connz: %v", err)
	}
	zc := snapshotAllViews(t, s, &SnapshotzOptions{
		Views: []string{SnapshotViewConnz},
		Connz: connzOpts,
	})
	gotConnz := zc.Connz
	gotConnz.Now = wantConnz.Now
	if len(gotConnz.Conns) != len(wantConnz.Conns) {
		t.Fatalf("connz length mismatch")
	}
	for i := range wantConnz.Conns {
		gotConnz.Conns[i].Uptime = wantConnz.Conns[i].Uptime
		gotConnz.Conns[i].Idle = wantConnz.Conns[i].Idle
		gotConnz.Conns[i].RTT = wantConnz.Conns[i].RTT
	}
	if !reflect.DeepEqual(wantConnz, gotConnz) {
		t.Fatalf("connz parity mismatch:\nwant: %+v\ngot:  %+v", wantConnz, gotConnz)
	}

	// Subsz
	subszOpts := &SubszOptions{Subscriptions: true}
	wantSubsz, err := s.Subsz(subszOpts)
	if err != nil {
		t.Fatalf("standalone subsz: %v", err)
	}
	zs := snapshotAllViews(t, s, &SnapshotzOptions{
		Views: []string{SnapshotViewSubsz},
		Subsz: subszOpts,
	})
	gotSubsz := zs.Subsz
	gotSubsz.Now = wantSubsz.Now
	// The sublist stores nodes in maps, so the standalone collection itself
	// has no cross-call order guarantee; compare as sorted multisets.
	sortSubs := func(l []SubDetail) {
		sort.Slice(l, func(i, j int) bool {
			if l[i].Cid != l[j].Cid {
				return l[i].Cid < l[j].Cid
			}
			return l[i].Subject < l[j].Subject
		})
	}
	sortSubs(wantSubsz.Subs)
	sortSubs(gotSubsz.Subs)
	if !reflect.DeepEqual(wantSubsz, gotSubsz) {
		t.Fatalf("subsz parity mismatch:\nwant: %+v\ngot:  %+v", wantSubsz, gotSubsz)
	}

	// Routez
	wantRoutez, err := s.Routez(&RoutezOptions{})
	if err != nil {
		t.Fatalf("standalone routez: %v", err)
	}
	zr := snapshotAllViews(t, s, &SnapshotzOptions{
		Views:  []string{SnapshotViewRoutez},
		Routez: &RoutezOptions{},
	})
	gotRoutez := zr.Routez
	gotRoutez.Now = wantRoutez.Now
	if !reflect.DeepEqual(wantRoutez, gotRoutez) {
		t.Fatalf("routez parity mismatch:\nwant: %+v\ngot:  %+v", wantRoutez, gotRoutez)
	}

	// Accountz list (order is map iteration order in both).
	wantAccountz, err := s.Accountz(&AccountzOptions{})
	if err != nil {
		t.Fatalf("standalone accountz: %v", err)
	}
	za := snapshotAllViews(t, s, &SnapshotzOptions{
		Views:    []string{SnapshotViewAccountz},
		Accountz: &AccountzOptions{},
	})
	gotAccountz := za.Accountz
	gotAccountz.Now = wantAccountz.Now
	sort.Strings(wantAccountz.Accounts)
	sort.Strings(gotAccountz.Accounts)
	if !reflect.DeepEqual(wantAccountz, gotAccountz) {
		t.Fatalf("accountz parity mismatch:\nwant: %+v\ngot:  %+v", wantAccountz, gotAccountz)
	}

	// Accountz detail for the global account.
	detailOpts := &AccountzOptions{Account: globalAccountName}
	wantDetail, err := s.Accountz(detailOpts)
	if err != nil {
		t.Fatalf("standalone accountz detail: %v", err)
	}
	zd := snapshotAllViews(t, s, &SnapshotzOptions{
		Views:    []string{SnapshotViewAccountz},
		Accountz: detailOpts,
	})
	gotDetail := zd.Accountz
	gotDetail.Now = wantDetail.Now
	if !reflect.DeepEqual(wantDetail, gotDetail) {
		t.Fatalf("accountz detail parity mismatch:\nwant: %+v\ngot:  %+v", wantDetail, gotDetail)
	}
}

// TestSnapshotzHTTP exercises the monitoring endpoint end to end, including
// parameter validation and strict add-only JSON on the standalone views.
func TestSnapshotzHTTP(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	base := fmt.Sprintf("http://127.0.0.1:%d", s.MonitorAddr().Port)

	body := readBody(t, base+SnapshotzPath+"?views=connz,subsz,routez")
	z := &Snapshotz{}
	if err := json.Unmarshal(body, z); err != nil {
		t.Fatalf("unmarshal snapshotz: %v\n%s", err, string(body))
	}
	if len(z.Views) != 3 || z.Connz == nil || z.Subsz == nil || z.Routez == nil || z.Accountz != nil {
		t.Fatalf("Unexpected views in response: %+v", z.Views)
	}
	if z.Budget.MaxItems != DefaultSnapshotMaxItems {
		t.Fatalf("Default budget missing over HTTP: %+v", z.Budget)
	}
	if z.ID == _EMPTY_ {
		t.Fatalf("Snapshot id missing over HTTP")
	}

	// Budget query parameter honored: one item across connz + accountz cuts
	// the second view.
	body = readBody(t, base+SnapshotzPath+"?views=connz,accountz&max_items=1")
	z = &Snapshotz{}
	if err := json.Unmarshal(body, z); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if z.Budget.MaxItems != 1 || !z.Budget.Truncated || z.Budget.ItemsReturned != 1 {
		t.Fatalf("max_items not honored over HTTP: %+v", z.Budget)
	}

	// Bad parameters.
	readBodyEx(t, base+SnapshotzPath+"?views=bogus", http.StatusBadRequest, textPlain)
	readBodyEx(t, base+SnapshotzPath+"?max_items=-1", http.StatusBadRequest, textPlain)
	readBodyEx(t, base+SnapshotzPath+"?max_bytes=-1", http.StatusBadRequest, textPlain)
	readBodyEx(t, base+SnapshotzPath+"?views=connz,connz", http.StatusBadRequest, textPlain)

	// Default (no views) collects all four.
	body = readBody(t, base+SnapshotzPath)
	z = &Snapshotz{}
	if err := json.Unmarshal(body, z); err != nil {
		t.Fatalf("unmarshal default snapshotz: %v", err)
	}
	if len(z.Views) != 4 {
		t.Fatalf("Expected all four views by default, got %v", z.Views)
	}

	// The standalone connz JSON must not contain any snapshot-only field.
	connzBody := string(readBody(t, base+ConnzPath))
	if strings.Contains(connzBody, "snapshot_truncated") {
		t.Fatalf("Standalone connz must not expose snapshot fields: %s", connzBody)
	}
}

func mustSub(t *testing.T, nc *nats.Conn, subject string) {
	t.Helper()
	if _, err := nc.Subscribe(subject, func(_ *nats.Msg) {}); err != nil {
		t.Fatalf("subscribe %q: %v", subject, err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush after %q: %v", subject, err)
	}
}

// TestSnapshotzConsistentUnderChurn holds the cross-view invariants while
// connections are created and torn down continuously.
func TestSnapshotzConsistentUnderChurn(t *testing.T) {
	s := runMonitorServerWithAccounts()
	defer s.Shutdown()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	churn := func(user string) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			func() {
				nc, err := nats.Connect(fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", user, user, s.Addr().(*net.TCPAddr).Port))
				if err != nil {
					return
				}
				defer nc.Close()
				if _, err := nc.Subscribe("foo.bar", func(_ *nats.Msg) {}); err == nil {
					nc.Flush()
				}
				time.Sleep(time.Millisecond)
			}()
		}
	}
	wg.Add(4)
	go churn("a")
	go churn("a")
	go churn("b")
	go churn("b")
	defer func() {
		close(stop)
		wg.Wait()
	}()

	assertIDs := func(t *testing.T, z *Snapshotz) {
		t.Helper()
		cids := make(map[uint64]struct{}, z.Connz.NumConns)
		for _, ci := range z.Connz.Conns {
			cids[ci.Cid] = struct{}{}
		}
		rids := make(map[uint64]struct{}, len(z.Routez.Routes))
		for _, ri := range z.Routez.Routes {
			rids[ri.Rid] = struct{}{}
		}
		for _, sd := range z.Subsz.Subs {
			if _, ok := cids[sd.Cid]; !ok {
				if _, ok := rids[sd.Cid]; !ok {
					t.Fatalf("subsz cid %d missing from connz and routez", sd.Cid)
				}
			}
		}
	}

	for i := 0; i < 60; i++ {
		// Full snapshot: subscription entities must exist in the connection
		// or route view of the same snapshot.
		z := snapshotAllViews(t, s, &SnapshotzOptions{
			Subsz: &SubszOptions{Subscriptions: true},
		})
		assertIDs(t, z)
		if z.Budget.Truncated {
			t.Fatalf("unexpected truncation under churn: %+v", z.Budget)
		}

		// Per-account snapshots: connz count and accountz count must match,
		// and every subscription detail must be attributed to that account.
		for _, acc := range []string{"A", "B"} {
			za, err := s.Snapshotz(&SnapshotzOptions{
				Views:    []string{SnapshotViewConnz, SnapshotViewSubsz, SnapshotViewAccountz},
				Connz:    &ConnzOptions{Account: acc},
				Subsz:    &SubszOptions{Subscriptions: true, Account: acc},
				Accountz: &AccountzOptions{Account: acc},
			})
			if err != nil {
				t.Fatalf("scoped snapshot for %s: %v", acc, err)
			}
			want := za.Accountz.Account.ClientCnt + za.Accountz.Account.LeafCnt
			if za.Connz.NumConns != want {
				t.Fatalf("account %s: connz has %d conns but accountz counts %d",
					acc, za.Connz.NumConns, want)
			}
			for _, sd := range za.Subsz.Subs {
				if sd.Account != acc {
					t.Fatalf("account %s snapshot leaked sub from %q", acc, sd.Account)
				}
				if _, ok := snapCidSet(za.Connz)[sd.Cid]; !ok {
					t.Fatalf("account %s sub cid %d missing from connz", acc, sd.Cid)
				}
			}
		}
	}
}

func snapCidSet(c *Connz) map[uint64]struct{} {
	set := make(map[uint64]struct{}, c.NumConns)
	for _, ci := range c.Conns {
		set[ci.Cid] = struct{}{}
	}
	return set
}
