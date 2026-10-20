/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tlsproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
)

const (
	// EnvReadyBackend is the plaintext address of Garage's admin API on the
	// loopback, which /ready asks; empty means /ready is not served.
	EnvReadyBackend = "TLS_READY_BACKEND"
	// EnvAdminToken is Garage's admin token, which GetNodeStatistics requires.
	EnvAdminToken = "TLS_ADMIN_TOKEN"

	// ReadyPath is where the readiness of the node is served, on the metrics
	// listener and without a token: the kubelet probes it.
	ReadyPath = "/ready"
)

// syncedTables are the metadata tables a node must hold before it takes S3
// traffic. Each is replicated in full to every node, and Garage resolves
// buckets and access keys from the local copy only, so a node missing entries
// answers "NoSuchBucket" or "No such key" for buckets and keys that exist.
var syncedTables = []string{"bucket_v2", "bucket_alias", "key"}

// readyTimeout bounds one readiness check: the kubelet gives the probe 5 s.
const readyTimeout = 4 * time.Second

// Readiness answers whether this Garage node may take traffic.
//
// Garage's /health says the node holds a role in the layout and reaches enough
// peers. It says nothing about the node's tables, and a node that comes back on
// an empty volume is healthy within a minute while its bucket and key tables
// are still empty: they are filled only by its peers pushing their entries to
// it. Until they have, part of the S3 requests the Service sends to it fail.
//
// So /ready also asks the admin API for every node's table statistics and holds
// the node back until it has, in each table above, at least as many entries as
// any peer that answered. Once that holds, it holds for the life of the
// process: the counts of the key table move with every temporary key, and a
// node that served would otherwise drop out of the Service over a key issued a
// moment ago on its peers. A node that restarts on its volume passes at once.
type Readiness struct {
	Backend string
	Token   string
	Log     logger.Logger

	client *http.Client
	synced atomic.Bool
}

// ServeHTTP answers 200 when the node is ready and 503 with the reason when not.
func (r *Readiness) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), readyTimeout)
	defer cancel()
	if err := r.check(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready\n"))
}

func (r *Readiness) check(ctx context.Context) error {
	if r.client == nil {
		r.client = &http.Client{}
	}
	if err := r.health(ctx); err != nil {
		return err
	}
	if r.synced.Load() || r.Token == "" {
		return nil
	}
	self, err := r.statistics(ctx, "self")
	if err != nil {
		return err
	}
	all, err := r.statistics(ctx, "*")
	if err != nil {
		return err
	}
	if err := tablesCaughtUp(self, all); err != nil {
		return err
	}
	r.synced.Store(true)
	r.Log.Info("the node holds every metadata entry its peers hold; it takes traffic from now on")
	return nil
}

func (r *Readiness) health(ctx context.Context) error {
	resp, err := r.get(ctx, "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("garage /health answers %s", resp.Status)
	}
	return nil
}

// nodeTables is the part of GetNodeStatistics this check reads, by node ID.
type nodeTables map[string]map[string]uint64

func (r *Readiness) statistics(ctx context.Context, node string) (nodeTables, error) {
	resp, err := r.get(ctx, "/v2/GetNodeStatistics?node="+node)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GetNodeStatistics?node=%s answers %s", node, resp.Status)
	}
	var body struct {
		Success map[string]struct {
			TableStats []struct {
				TableName string `json:"tableName"`
				Items     uint64 `json:"items"`
			} `json:"tableStats"`
		} `json:"success"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("read GetNodeStatistics?node=%s: %w", node, err)
	}
	out := nodeTables{}
	for id, stats := range body.Success {
		tables := map[string]uint64{}
		for _, t := range stats.TableStats {
			tables[t.TableName] = t.Items
		}
		out[id] = tables
	}
	return out, nil
}

func (r *Readiness) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+r.Backend+path, nil)
	if err != nil {
		return nil, err
	}
	if r.Token != "" {
		req.Header.Set("Authorization", "Bearer "+r.Token)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("garage admin API: %w", err)
	}
	return resp, nil
}

// tablesCaughtUp reports why the local node is not yet as complete as its peers,
// or nil once it is. self holds the local node only; all holds every node that
// answered, the local one among them. Peers that did not answer are not waited
// for: /health already says enough of them are up.
func tablesCaughtUp(self, all nodeTables) error {
	if len(self) != 1 {
		return fmt.Errorf("GetNodeStatistics?node=self answered for %d nodes", len(self))
	}
	var id string
	var local map[string]uint64
	for id, local = range self {
	}
	for peer, tables := range all {
		if peer == id {
			continue
		}
		for _, table := range syncedTables {
			if tables[table] > local[table] {
				return fmt.Errorf("table %s holds %d entries here and %d on node %.16s; waiting for the peers to push the rest",
					table, local[table], tables[table], peer)
			}
		}
	}
	return nil
}
