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
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nuid"
)

// View selectors accepted by Snapshotz.
const (
	SnapshotViewConnz    = "connz"
	SnapshotViewSubsz    = "subsz"
	SnapshotViewRoutez   = "routez"
	SnapshotViewAccountz = "accountz"
)

// snapshotKnownViews is the canonical order used when no explicit view list
// is supplied and also the set of valid view names.
var snapshotKnownViews = []string{
	SnapshotViewConnz,
	SnapshotViewSubsz,
	SnapshotViewRoutez,
	SnapshotViewAccountz,
}

// Shared collection budget defaults. The budget always applies; these only
// fill in values the caller did not specify.
const (
	// DefaultSnapshotMaxItems is the default ceiling on the total number of
	// entries returned across all views of one snapshot.
	DefaultSnapshotMaxItems = 100_000

	// DefaultSnapshotMaxBytes is the default ceiling, in bytes, on the total
	// compact JSON size of the entries returned across all views of one
	// snapshot.
	DefaultSnapshotMaxBytes = 32 * 1024 * 1024
)

// Reasons recorded in SnapshotTruncation.Reason and SnapshotBudget.LimitedBy.
const (
	snapshotLimitedByItems = "max_items"
	snapshotLimitedByBytes = "max_bytes"
)

// SnapshotTruncation annotates a view inside a Snapshotz response when the
// shared collection budget prevented it from containing all entries it had
// collected. It is only ever present on views returned as part of a snapshot;
// standalone view responses never contain it.
type SnapshotTruncation struct {
	// Reason is the budget that caused the truncation: max_items or max_bytes.
	Reason string `json:"reason"`

	// Returned is the number of entries from this view contained in the
	// response.
	Returned int `json:"returned"`

	// Available is the number of entries the view had collected before the
	// shared snapshot budget was applied. The view's own limit/offset
	// semantics were already applied at that point.
	Available int `json:"available"`

	// Dropped is Available - Returned.
	Dropped int `json:"dropped"`
}

// SnapshotBudget reports the shared collection budget attached to a snapshot
// and how much of it the returned snapshot consumed.
type SnapshotBudget struct {
	// MaxItems is the declared ceiling on total returned entries.
	MaxItems int `json:"max_items"`

	// MaxBytes is the declared ceiling on the total compact JSON size of the
	// returned entries.
	MaxBytes int `json:"max_bytes"`

	// ItemsReturned is the number of entries actually returned, summed across
	// all views. It never exceeds MaxItems.
	ItemsReturned int `json:"items_returned"`

	// BytesReturned is the sum of the compact JSON encodings of the returned
	// entries. It never exceeds MaxBytes.
	BytesReturned int `json:"bytes_returned"`

	// Truncated is true if at least one view was truncated by the budget.
	Truncated bool `json:"truncated"`

	// LimitedBy records the first budget that was reached: max_items or
	// max_bytes. Empty when nothing was truncated.
	LimitedBy string `json:"limited_by,omitempty"`
}

// SnapshotzOptions are the options passed to Snapshotz. The per-view option
// structs use exactly the same semantics as the standalone view endpoints;
// the snapshot only adds view selection and the shared collection budget.
type SnapshotzOptions struct {
	// Views selects the views to collect, e.g. ["connz","subsz","routez"].
	// When empty, all known views are collected in canonical order. Each view
	// may appear at most once.
	Views []string `json:"views,omitempty"`

	// Per-view options. Nil means the same defaults the standalone endpoint
	// applies.
	Connz    *ConnzOptions    `json:"connz,omitempty"`
	Subsz    *SubszOptions    `json:"subsz,omitempty"`
	Routez   *RoutezOptions   `json:"routez,omitempty"`
	Accountz *AccountzOptions `json:"accountz,omitempty"`

	// MaxItems bounds the total number of entries returned across all views.
	// Zero means DefaultSnapshotMaxItems; negative values are an error.
	MaxItems int `json:"max_items,omitempty"`

	// MaxBytes bounds the total compact JSON entry size returned across all
	// views. Zero means DefaultSnapshotMaxBytes; negative values are an error.
	MaxBytes int `json:"max_bytes,omitempty"`

	// ctx is the request context. Collection stops as soon as it is canceled;
	// nothing is registered on the server so canceling only releases the
	// per-request state.
	ctx context.Context
}

// Snapshotz is a point-in-time collection of several monitoring views. Every
// embedded view is derived from the same frozen runtime state and therefore
// shares the same logical instant, server id and timestamp.
type Snapshotz struct {
	// ID identifies this snapshot. It only ever exists in this response:
	// snapshots are neither stored nor published on failure, so a failed ID
	// can never be referenced.
	ID string `json:"snapshot_id"`

	ServerID string    `json:"server_id"`
	Now      time.Time `json:"now"`

	// Views is the effective, canonicalized list of collected views and is
	// the order in which the shared budget is consumed.
	Views []string `json:"views"`

	// Budget describes the shared collection budget and its consumption.
	Budget SnapshotBudget `json:"budget"`

	Connz    *Connz    `json:"connz,omitempty"`
	Subsz    *Subsz    `json:"subsz,omitempty"`
	Routez   *Routez   `json:"routez,omitempty"`
	Accountz *Accountz `json:"accountz,omitempty"`
}

// monitorSnapshot is the immutable per-request state captured at one logical
// instant. It is intentionally never stored on the Server: when the request
// finishes, fails or is canceled, it is unreachable and garbage collected,
// which is what makes concurrent snapshots mutually independent.
type monitorSnapshot struct {
	id         string
	now        time.Time
	serverID   string
	serverName string

	// clients is the frozen equivalent of Connz' global client view,
	// s.clients ∪ s.leafs, copied under s.mu.
	clients map[uint64]*client

	// routes is the frozen route list in forEachRoute order (non per-account
	// routes followed by per-account routes).
	routes []*client

	// entities is the set of connections a subscription detail is allowed to
	// reference: open clients, leafnodes and routes. It is the basis of the
	// cross-view existence guarantee.
	entities map[*client]struct{}

	// closed is the frozen contents of the closed-connections ring.
	closed []*closedClient

	// accounts is the frozen list of account names.
	accounts []string

	// systemAccount is the frozen system account name, empty when none.
	systemAccount string

	// subs is the frozen union of all account sublists' local subscriptions,
	// gathered exactly like Subsz does.
	subs []*subscription

	// slStats holds the frozen per-account sublist statistics.
	slStats map[string]*SublistStats

	// clientAcc is the account name of every frozen client, read exactly
	// once. Both account-scoped filtering and account count derivation use
	// it, so they can never disagree even during a client's registration.
	clientAcc map[*client]string

	// clientCnt and leafCnt are derived per account from the frozen entity
	// set so account counts match the connection listings exactly.
	clientCnt map[string]int
	leafCnt   map[string]int
}

// freezeMonitorState captures, as close to a single instant as possible, the
// slices of runtime state all snapshot views will be derived from. Only
// pointer identities are captured while locks are held; all per-entity data
// is read afterwards, exactly as the standalone view collection already
// does. No lock is held when the function returns.
func (s *Server) freezeMonitorState(ctx context.Context) (*monitorSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	snap := &monitorSnapshot{
		id:       nuid.Next(),
		now:      time.Now().UTC(),
		clients:  make(map[uint64]*client),
		entities: make(map[*client]struct{}),
	}

	// One critical section on the server lock freezes the global connection
	// and route identity sets.
	s.mu.RLock()
	snap.serverID = s.info.ID
	snap.serverName = s.info.Name
	snap.clients = make(map[uint64]*client, len(s.clients)+len(s.leafs))
	maps.Copy(snap.clients, s.clients)
	maps.Copy(snap.clients, s.leafs)
	for _, c := range snap.clients {
		snap.entities[c] = struct{}{}
	}
	// Mirror forEachRoute: non per-account routes then per-account routes.
	s.forEachNonPerAccountRoute(func(r *client) {
		snap.routes = append(snap.routes, r)
		snap.entities[r] = struct{}{}
	})
	for _, conns := range s.accRoutes {
		for _, r := range conns {
			snap.routes = append(snap.routes, r)
			snap.entities[r] = struct{}{}
		}
	}
	s.mu.RUnlock()

	// Closed connections come from their own ring lock.
	if s.closed != nil {
		snap.closed = s.closed.closedClients()
	}

	if sacc := s.SystemAccount(); sacc != nil {
		snap.systemAccount = sacc.GetName()
	}

	// Freeze per-account identities, sublist contents and stats.
	snap.accounts = make([]string, 0)
	snap.slStats = make(map[string]*SublistStats)
	var subs []*subscription
	var ctxErr error
	s.accounts.Range(func(_, v any) bool {
		if ctxErr = ctx.Err(); ctxErr != nil {
			return false
		}
		acc := v.(*Account)
		name := acc.GetName()
		snap.accounts = append(snap.accounts, name)
		snap.slStats[name] = acc.sl.Stats()
		acc.sl.localSubs(&subs, false)
		return true
	})
	if ctxErr != nil {
		return nil, ctxErr
	}
	snap.subs = subs

	// Read each client's account attribution exactly once, and derive
	// per-account connection counts from the SAME frozen identity set the
	// connection view uses. kind is immutable; read c.acc under the client
	// lock like Connz does.
	snap.clientAcc = make(map[*client]string, len(snap.clients))
	snap.clientCnt = make(map[string]int)
	snap.leafCnt = make(map[string]int)
	for _, c := range snap.clients {
		c.mu.RLock()
		var name string
		if c.acc != nil {
			name = c.acc.Name
		}
		c.mu.RUnlock()
		snap.clientAcc[c] = name
		if name == _EMPTY_ {
			continue
		}
		switch c.kind {
		case CLIENT:
			snap.clientCnt[name]++
		case LEAF:
			snap.leafCnt[name]++
		}
		// Other kinds mirror numLocalConnections(): system/internal
		// connections are excluded from client and leaf counts.
	}
	// Routes are not clients and never count toward account connection
	// totals, but their propagated subscriptions live in account sublists,
	// so their attribution is needed for account-scoped subsz filtering.
	for _, r := range snap.routes {
		r.mu.RLock()
		var name string
		if r.acc != nil {
			name = r.acc.Name
		}
		r.mu.RUnlock()
		snap.clientAcc[r] = name
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return snap, nil
}

// snapshotClosedClients returns the closed-connection list captured at freeze
// time, or the current ring contents when no snapshot is in flight.
func snapshotClosedClients(snap *monitorSnapshot, s *Server) []*closedClient {
	if snap != nil {
		return snap.closed
	}
	return s.closed.closedClients()
}

// aggregateSublistStats folds the frozen per-account stats into dst, applying
// the same optional account filter the standalone Subsz collection applies.
func (snap *monitorSnapshot) aggregateSublistStats(dst *SublistStats, filterAcc string) {
	for _, name := range snap.accounts {
		if filterAcc != _EMPTY_ && name != filterAcc {
			continue
		}
		if st := snap.slStats[name]; st != nil {
			dst.add(st)
		}
	}
}

// Snapshotz collects the requested monitoring views from a single frozen
// point in time and enforces a shared collection budget.
//
// Guarantees:
//   - All embedded views share the same logical instant, timestamp and server
//     id. An entity present in the frozen state is present in every view that
//     lists that kind of entity; an entity absent from it is absent from all.
//   - Existing per-view field names, meanings, ordering, limits and offset
//     semantics are unchanged. The shared budget is an additional ceiling on
//     top; every budget-induced cut is reported with a SnapshotTruncation
//     marker instead of silently omitting entries.
//   - Collection is all-or-nothing: if any view fails, no snapshot is
//     returned and all per-request state is released. The snapshot id only
//     exists in a successful response and can never be referenced after a
//     failure or cancellation.
//   - Nothing is stored on the server and nothing is written to disk, so
//     concurrent snapshots cannot affect each other.
func (s *Server) Snapshotz(opts *SnapshotzOptions) (*Snapshotz, error) {
	if opts == nil {
		opts = &SnapshotzOptions{}
	}
	ctx := opts.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	views, err := normalizeSnapshotViews(opts.Views)
	if err != nil {
		return nil, err
	}
	maxItems, maxBytes, err := normalizeSnapshotBudget(opts)
	if err != nil {
		return nil, err
	}

	snap, err := s.freezeMonitorState(ctx)
	if err != nil {
		return nil, err
	}

	res := &Snapshotz{
		ID:       snap.id,
		ServerID: snap.serverID,
		Now:      snap.now,
		Views:    views,
		Budget: SnapshotBudget{
			MaxItems: maxItems,
			MaxBytes: maxBytes,
		},
	}

	// Collect every view from the same frozen state. Copies of the caller's
	// option structs are used so the frozen pointer never escapes into them.
	// Any failure is fatal to the whole snapshot.
	for _, view := range views {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch view {
		case SnapshotViewConnz:
			copts := ConnzOptions{}
			if opts.Connz != nil {
				copts = *opts.Connz
			}
			copts.snap = snap
			v, err := s.Connz(&copts)
			if err != nil {
				return nil, err
			}
			res.Connz = v
		case SnapshotViewSubsz:
			sopts := SubszOptions{}
			if opts.Subsz != nil {
				sopts = *opts.Subsz
			}
			sopts.snap = snap
			v, err := s.Subsz(&sopts)
			if err != nil {
				return nil, err
			}
			res.Subsz = v
		case SnapshotViewRoutez:
			ropts := RoutezOptions{}
			if opts.Routez != nil {
				ropts = *opts.Routez
			}
			ropts.snap = snap
			v, err := s.Routez(&ropts)
			if err != nil {
				return nil, err
			}
			res.Routez = v
		case SnapshotViewAccountz:
			aopts := AccountzOptions{}
			if opts.Accountz != nil {
				aopts = *opts.Accountz
			}
			aopts.snap = snap
			v, err := s.Accountz(&aopts)
			if err != nil {
				return nil, err
			}
			res.Accountz = v
		default:
			// normalizeSnapshotViews rejects unknown views.
			return nil, fmt.Errorf("unknown snapshot view: %q", view)
		}
	}

	if err := res.enforceSnapshotBudget(ctx); err != nil {
		return nil, err
	}
	return res, nil
}

// normalizeSnapshotViews validates and canonicalizes the requested view list.
func normalizeSnapshotViews(requested []string) ([]string, error) {
	if len(requested) == 0 {
		return slices.Clone(snapshotKnownViews), nil
	}
	seen := make(map[string]struct{}, len(requested))
	views := make([]string, 0, len(requested))
	for _, v := range requested {
		name := strings.ToLower(strings.TrimSpace(v))
		if !slices.Contains(snapshotKnownViews, name) {
			return nil, fmt.Errorf("unknown snapshot view: %q", v)
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate snapshot view: %q", name)
		}
		seen[name] = struct{}{}
		views = append(views, name)
	}
	return views, nil
}

// normalizeSnapshotBudget resolves the shared budget, applying defaults to
// unset values.
func normalizeSnapshotBudget(opts *SnapshotzOptions) (int, int, error) {
	maxItems := opts.MaxItems
	if maxItems == 0 {
		maxItems = DefaultSnapshotMaxItems
	}
	if maxItems < 0 {
		return 0, 0, fmt.Errorf("snapshot max_items must be greater than zero")
	}
	maxBytes := opts.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSnapshotMaxBytes
	}
	if maxBytes < 0 {
		return 0, 0, fmt.Errorf("snapshot max_bytes must be greater than zero")
	}
	return maxItems, maxBytes, nil
}

// snapshotBudgetView adapts one embedded view to the shared budget algorithm.
type snapshotBudgetView struct {
	name      string
	available int
	// entry returns the i-th collected entry for byte accounting.
	entry func(i int) any
	// trim cuts the view down to its first n entries (its existing ordering)
	// and keeps its own count fields consistent with that.
	trim func(n int)
	// mark attaches the truncation annotation to the view.
	mark func(*SnapshotTruncation)
}

// budgetViews returns adapters in the order views must consume the shared
// budget.
func (z *Snapshotz) budgetViews() []snapshotBudgetView {
	views := make([]snapshotBudgetView, 0, len(z.Views))
	for _, view := range z.Views {
		switch view {
		case SnapshotViewConnz:
			if z.Connz == nil {
				continue
			}
			c := z.Connz
			views = append(views, snapshotBudgetView{
				name:      SnapshotViewConnz,
				available: len(c.Conns),
				entry:     func(i int) any { return c.Conns[i] },
				trim: func(n int) {
					c.Conns = c.Conns[:n]
					c.NumConns = n
				},
				mark: func(t *SnapshotTruncation) { c.SnapshotTrunc = t },
			})
		case SnapshotViewSubsz:
			if z.Subsz == nil {
				continue
			}
			st := z.Subsz
			views = append(views, snapshotBudgetView{
				name:      SnapshotViewSubsz,
				available: len(st.Subs),
				entry:     func(i int) any { return st.Subs[i] },
				trim:      func(n int) { st.Subs = st.Subs[:n] },
				mark:      func(t *SnapshotTruncation) { st.SnapshotTrunc = t },
			})
		case SnapshotViewRoutez:
			if z.Routez == nil {
				continue
			}
			rs := z.Routez
			views = append(views, snapshotBudgetView{
				name:      SnapshotViewRoutez,
				available: len(rs.Routes),
				entry:     func(i int) any { return rs.Routes[i] },
				trim:      func(n int) { rs.Routes = rs.Routes[:n] },
				mark:      func(t *SnapshotTruncation) { rs.SnapshotTrunc = t },
			})
		case SnapshotViewAccountz:
			if z.Accountz == nil {
				continue
			}
			az := z.Accountz
			if az.Account != nil {
				// Detail mode: the single account detail is one entry.
				views = append(views, snapshotBudgetView{
					name:      SnapshotViewAccountz,
					available: 1,
					entry:     func(_ int) any { return az.Account },
					trim: func(n int) {
						if n == 0 {
							az.Account = nil
						}
					},
					mark: func(t *SnapshotTruncation) { az.SnapshotTrunc = t },
				})
			} else {
				views = append(views, snapshotBudgetView{
					name:      SnapshotViewAccountz,
					available: len(az.Accounts),
					entry:     func(i int) any { return az.Accounts[i] },
					trim: func(n int) {
						trimmed := az.Accounts[:n]
						if n == 0 {
							// Preserve the standalone empty-but-non-nil JSON.
							trimmed = []string{}
						}
						az.Accounts = trimmed
					},
					mark: func(t *SnapshotTruncation) { az.SnapshotTrunc = t },
				})
			}
		}
	}
	return views
}

// enforceSnapshotBudget applies the shared item and byte ceilings in declared
// view order. Each view keeps a prefix of its entries in its existing order.
// The item ceiling is applied first; the byte ceiling is then checked entry
// by entry using the compact JSON encoding of each entry. Every cut is
// annotated on the view and reflected in Budget.
func (z *Snapshotz) enforceSnapshotBudget(ctx context.Context) error {
	b := &z.Budget
	remainingItems := b.MaxItems
	remainingBytes := b.MaxBytes
	limitedBy := ""

	for _, v := range z.budgetViews() {
		if err := ctx.Err(); err != nil {
			return err
		}
		avail := v.available
		if avail == 0 {
			continue
		}

		// Budget already exhausted by an earlier view: this view contributes
		// nothing but is still explicitly marked as truncated.
		if limitedBy != "" {
			v.trim(0)
			v.mark(&SnapshotTruncation{
				Reason:    limitedBy,
				Returned:  0,
				Available: avail,
				Dropped:   avail,
			})
			b.Truncated = true
			continue
		}

		// Item ceiling first.
		n := min(avail, remainingItems)
		reason := ""
		if n < avail {
			reason = snapshotLimitedByItems
		}

		// Byte ceiling second, checked per entry in existing order.
		used := 0
		for i := 0; i < n; i++ {
			eb, err := json.Marshal(v.entry(i))
			if err != nil {
				return fmt.Errorf("snapshot view %s: %w", v.name, err)
			}
			cost := len(eb)
			if used+cost > remainingBytes {
				n = i
				if reason == "" {
					reason = snapshotLimitedByBytes
				}
				break
			}
			used += cost
		}

		v.trim(n)
		b.ItemsReturned += n
		b.BytesReturned += used
		remainingItems -= n
		remainingBytes -= used

		if reason != "" {
			v.mark(&SnapshotTruncation{
				Reason:    reason,
				Returned:  n,
				Available: avail,
				Dropped:   avail - n,
			})
			b.Truncated = true
			if b.LimitedBy == "" {
				b.LimitedBy = reason
			}
			limitedBy = reason
		}
	}
	return nil
}

// HandleSnapshotz processes HTTP requests collecting multiple monitoring
// views at one logical instant.
func (s *Server) HandleSnapshotz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[SnapshotzPath]++
	s.mu.Unlock()

	q := r.URL.Query()

	opts := &SnapshotzOptions{ctx: r.Context()}
	if views := strings.TrimSpace(q.Get("views")); views != _EMPTY_ {
		opts.Views = strings.Split(views, ",")
	}

	// Shared options, decoded with the exact same semantics as the
	// standalone endpoints.
	sortOpt := SortOpt(q.Get("sort"))
	auth, err := decodeBool(w, r, "auth")
	if err != nil {
		return
	}
	subs, subsDet, err := decodeSubs(w, r)
	if err != nil {
		return
	}
	offset, err := decodeInt(w, r, "offset")
	if err != nil {
		return
	}
	limit, err := decodeInt(w, r, "limit")
	if err != nil {
		return
	}
	cid, err := decodeUint64(w, r, "cid")
	if err != nil {
		return
	}
	state, err := decodeState(w, r)
	if err != nil {
		return
	}
	maxItems, err := decodeInt(w, r, "max_items")
	if err != nil {
		return
	}
	maxBytes, err := decodeInt(w, r, "max_bytes")
	if err != nil {
		return
	}

	user := q.Get("user")
	// The shared "acc" scopes connz and subsz and, when accountz is
	// requested, selects the account detail.
	acc := q.Get("acc")
	mqttCID := q.Get("mqtt_client")
	testSub := q.Get("test")

	opts.MaxItems = maxItems
	opts.MaxBytes = maxBytes
	opts.Connz = &ConnzOptions{
		Sort:                sortOpt,
		Username:            auth,
		Subscriptions:       subs,
		SubscriptionsDetail: subsDet,
		Offset:              offset,
		Limit:               limit,
		CID:                 cid,
		MQTTClient:          mqttCID,
		State:               state,
		User:                user,
		Account:             acc,
	}
	opts.Subsz = &SubszOptions{
		Subscriptions: subs || subsDet,
		Offset:        offset,
		Limit:         limit,
		Account:       acc,
		Test:          testSub,
	}
	opts.Routez = &RoutezOptions{
		Subscriptions:       subs,
		SubscriptionsDetail: subsDet,
	}
	opts.Accountz = &AccountzOptions{Account: acc}

	z, err := s.Snapshotz(opts)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	b, err := json.MarshalIndent(z, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to %s request: %v", SnapshotzPath, err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	ResponseHandler(w, r, b)
}
