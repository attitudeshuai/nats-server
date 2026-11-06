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
	"testing"

	"github.com/nats-io/nats.go"
)

// The disabled hook must be allocation free: one atomic pointer load and a
// nil branch.
func BenchmarkPubStatsHookDisabled(b *testing.B) {
	s := &Server{}
	acc := &Account{Name: "a"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.accountPublishStats(acc, "s", 4)
	}
}

// Enabled hot path for an already-tracked subject: RLock + map lookup +
// atomic adds.
func BenchmarkPubStatsHookEnabled(b *testing.B) {
	s := &Server{}
	s.pubStats.Store(newPubStatsMgr(defaultPublishStatsMaxSubjects))
	acc := &Account{Name: "a"}
	s.accountPublishStats(acc, "s", 4) // pre-create the entry
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.accountPublishStats(acc, "s", 4)
	}
}

// End-to-end publish cost with the ledger disabled vs enabled.
func BenchmarkPubStatsE2EPublish(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			if b.N < 10_000 {
				b.ResetTimer()
				return
			}
			opts := DefaultTestOptions
			opts.Port = -1
			opts.PublishStats.Enabled = enabled
			s := RunServer(&opts)
			defer s.Shutdown()

			nc, err := nats.Connect(s.ClientURL())
			if err != nil {
				b.Fatalf("connect: %v", err)
			}
			defer nc.Close()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := nc.Publish("bench.subject", []byte("0123456789")); err != nil {
					b.Fatalf("publish: %v", err)
				}
				if i%1000 == 0 {
					if err := nc.Flush(); err != nil {
						b.Fatalf("flush: %v", err)
					}
				}
			}
			if err := nc.Flush(); err != nil {
				b.Fatalf("flush: %v", err)
			}
		})
	}
}
