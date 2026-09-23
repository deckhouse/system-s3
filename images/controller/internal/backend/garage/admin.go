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

package garage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// adminClient is a minimal client for the Garage v2 admin API (served on the
// admin port behind a Bearer token). Garage v2 reworked the admin API: every
// call is `METHOD /v2/<CallName>` with the call name in the path, proper HTTP
// 404s for missing resources, and DeleteKey/DeleteBucket/UpdateBucket as POST
// with the id in a query parameter.
//
// NOTE: the request/response shapes below target the documented Garage v2.4.1
// admin API; the wire format is deliberately localised here so a fix stays in
// one file. The exported method set and the returned Go structs are unchanged
// from the v1 client, so the callers (mesh.go, rebalance.go, buckets.go,
// access.go) are not affected by the v2 migration.
//
// The v2 specification (doc/api/garage-admin-v2.json) is byte-for-byte the same
// set of 49 calls in v2.3.0 and v2.4.1 — same paths, same schemas — which is why
// the version bump touches nothing here beyond this comment.
type adminClient struct {
	http    *http.Client
	baseURL string // e.g. http://host:3903
	token   string
}

func newAdminClient(baseURL, token string) *adminClient {
	return &adminClient{
		http:    &http.Client{Timeout: 10 * time.Second},
		baseURL: baseURL,
		token:   token,
	}
}

// statusResponse carries the node identity of the queried Garage node (its own
// node id), used to seed the RPC mesh and layout.
type statusResponse struct {
	Node string
}

// layoutRole is one assigned role in the cluster layout.
type layoutRole struct {
	ID       string   `json:"id"`
	Zone     string   `json:"zone"`
	Capacity *int64   `json:"capacity"`
	Tags     []string `json:"tags"`
}

// clusterLayout mirrors GET /v2/GetClusterLayout.
type clusterLayout struct {
	Version int          `json:"version"`
	Roles   []layoutRole `json:"roles"`
}

// healthResponse is GET /v2/GetClusterHealth.
type healthResponse struct {
	Status         string `json:"status"` // healthy | degraded | unavailable
	KnownNodes     int    `json:"knownNodes"`
	ConnectedNodes int    `json:"connectedNodes"`
	StorageNodes   int    `json:"storageNodes"`
	// StorageNodesOk keeps the v1 Go field name used by callers; in v2 the JSON
	// field was renamed to storageNodesUp.
	StorageNodesOk   int `json:"storageNodesUp"`
	Partitions       int `json:"partitions"`
	PartitionsQuorum int `json:"partitionsQuorum"`
	PartitionsAllOk  int `json:"partitionsAllOk"`
}

// roleChange is one layout mutation the reconciler wants to make. It is the
// caller-facing intent type (mesh.go); stageLayout translates it to the v2
// UpdateClusterLayout wire shape. For an assignment set Zone/Capacity/Tags; to
// drop a node set Remove=true.
type roleChange struct {
	ID       string
	Zone     string
	Capacity *int64
	Tags     []string
	Remove   bool
}

// nodeInfoResponse is GET /v2/GetNodeInfo (a MultiResponse keyed by node id).
type nodeInfoResponse struct {
	Success map[string]struct {
		NodeID string `json:"nodeId"`
	} `json:"success"`
}

// status returns the queried node's own identity via GET /v2/GetNodeInfo?node=self.
func (c *adminClient) status(ctx context.Context) (*statusResponse, error) {
	var out nodeInfoResponse
	if err := c.do(ctx, http.MethodGet, "/v2/GetNodeInfo?node=self", nil, &out); err != nil {
		return nil, err
	}
	for _, v := range out.Success {
		if v.NodeID != "" {
			return &statusResponse{Node: v.NodeID}, nil
		}
	}
	return &statusResponse{}, nil
}

func (c *adminClient) health(ctx context.Context) (*healthResponse, error) {
	var out healthResponse
	if err := c.do(ctx, http.MethodGet, "/v2/GetClusterHealth", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// connect asks the node to connect to the given peers ("<id>@<host>:<port>").
func (c *adminClient) connect(ctx context.Context, peers []string) error {
	return c.do(ctx, http.MethodPost, "/v2/ConnectClusterNodes", peers, nil)
}

func (c *adminClient) layout(ctx context.Context) (*clusterLayout, error) {
	var out clusterLayout
	if err := c.do(ctx, http.MethodGet, "/v2/GetClusterLayout", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// stageLayout stages role changes. v2 UpdateClusterLayout takes an object
// {roles: [...]} where each entry is either an assignment (id+zone+capacity+tags)
// or a removal (id+remove:true) — the two variants must not be mixed on one entry.
func (c *adminClient) stageLayout(ctx context.Context, changes []roleChange) error {
	roles := make([]any, 0, len(changes))
	for _, ch := range changes {
		if ch.Remove {
			roles = append(roles, map[string]any{"id": ch.ID, "remove": true})
			continue
		}
		roles = append(roles, map[string]any{
			"id":       ch.ID,
			"zone":     ch.Zone,
			"capacity": ch.Capacity,
			"tags":     ch.Tags,
		})
	}
	return c.do(ctx, http.MethodPost, "/v2/UpdateClusterLayout", map[string]any{"roles": roles}, nil)
}

// applyLayout applies the staged changes, producing layout version `version`.
func (c *adminClient) applyLayout(ctx context.Context, version int) error {
	return c.do(ctx, http.MethodPost, "/v2/ApplyClusterLayout", map[string]int{"version": version}, nil)
}

// do performs an authenticated JSON request and optionally decodes the body.
func (c *adminClient) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil && (resp.StatusCode >= 200 && resp.StatusCode < 300) {
		// A connection cut mid-body used to surface as a JSON decode failure on a
		// truncated document, which reads like a protocol mismatch and sends the
		// reader to the wrong place. On a non-2xx the partial body is still the best
		// description of the failure, so it is kept for the apiError below.
		return fmt.Errorf("garage admin %s %s: read response body: %w", method, path, readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &apiError{Method: method, Path: path, StatusCode: resp.StatusCode, Body: string(data)}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("garage admin %s %s: decode: %w", method, path, err)
		}
	}
	return nil
}

// apiError is returned for non-2xx admin API responses.
type apiError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("garage admin %s %s: status %d: %s", e.Method, e.Path, e.StatusCode, e.Body)
}

// isNotFound reports whether err means "the resource does not exist". Garage v2
// returns a proper HTTP 404 for a missing key/bucket; the legacy 400-with-
// "not found"-body case is still tolerated defensively.
func isNotFound(err error) bool {
	var ae *apiError
	if !errors.As(err, &ae) {
		return false
	}
	if ae.StatusCode == http.StatusNotFound {
		return true
	}
	return ae.StatusCode == http.StatusBadRequest && strings.Contains(strings.ToLower(ae.Body), "not found")
}

// layoutHistory is the part of GET /v2/GetClusterLayoutHistory this module reads:
// which layout version is current, and the oldest version every node is known to
// have acknowledged.
type layoutHistory struct {
	CurrentVersion int64 `json:"currentVersion"`
	MinAck         int64 `json:"minAck"`
}

// layoutHistory reads how far the cluster has got in agreeing on the layout.
//
// MinAck below CurrentVersion means some node has not acknowledged the layout
// the others are already using. That is normal for a few seconds after a change
// and permanent for a node that is never coming back.
func (c *adminClient) layoutHistory(ctx context.Context) (*layoutHistory, error) {
	var out layoutHistory
	if err := c.do(ctx, http.MethodGet, "/v2/GetClusterLayoutHistory", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// skipDeadNodesResult names the nodes whose trackers were advanced.
type skipDeadNodesResult struct {
	AckUpdated  []string `json:"ackUpdated"`
	SyncUpdated []string `json:"syncUpdated"`
}

// skipDeadNodes advances the layout trackers past nodes that are not answering,
// so a node that is never coming back stops holding the cluster on an old layout
// version.
//
// allowMissingData is the dangerous half and this module never sets it: with it
// false, only the ACK tracker moves — the cluster stops WAITING for the dead
// node, and does not pretend the data it held has been accounted for. Moving the
// SYNC tracker is the operator's decision to accept a loss, not a controller's.
func (c *adminClient) skipDeadNodes(ctx context.Context, version int64, allowMissingData bool) (*skipDeadNodesResult, error) {
	body := map[string]any{"version": version, "allowMissingData": allowMissingData}
	var out skipDeadNodesResult
	if err := c.do(ctx, http.MethodPost, "/v2/ClusterLayoutSkipDeadNodes", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- Bucket / key management ------------------------------------------------

// keyInfo is the response of POST /v2/CreateKey and GET /v2/GetKeyInfo.
// secretAccessKey is only populated by the server at creation time (or when
// GetKeyInfo is asked with showSecretKey=true).
type keyInfo struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	Name            string `json:"name"`
}

// bucketInfo is the response of POST /v2/CreateBucket and GET /v2/GetBucketInfo.
//
// Bytes/Objects are what the bucket holds and Quotas what it is allowed to hold;
// both are absent from a CreateBucket response, which is why nothing reads them
// off one.
type bucketInfo struct {
	ID            string          `json:"id"`
	GlobalAliases []string        `json:"globalAliases"`
	Bytes         int64           `json:"bytes"`
	Objects       int64           `json:"objects"`
	Quotas        bucketQuotas    `json:"quotas"`
	Keys          []bucketKeyPerm `json:"keys"`
}

// bucketKeyPerm is one key's access to a bucket, as GetBucketInfo reports it.
// It is how the cutover finds the keys whose write access has to be revoked:
// the module knows which keys it issued, but the bucket knows which keys can
// actually write to it.
type bucketKeyPerm struct {
	AccessKeyID string      `json:"accessKeyId"`
	Name        string      `json:"name"`
	Permissions permissions `json:"permissions"`
}

// bucketQuotas is the quota pair Garage keeps per bucket. Both halves are
// nullable — null is "no limit" — and this module sets only the size.
type bucketQuotas struct {
	MaxSize    *int64 `json:"maxSize"`
	MaxObjects *int64 `json:"maxObjects"`
}

// keyListItem is one entry of GET /v2/ListKeys.
type keyListItem struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Expired    bool   `json:"expired"`
	Expiration string `json:"expiration"`
}

// permissions is the access granted to a key on a bucket.
type permissions struct {
	Read  bool `json:"read"`
	Write bool `json:"write"`
	Owner bool `json:"owner"`
}

// createKey creates a new access key with the given display name.
func (c *adminClient) createKey(ctx context.Context, name string) (*keyInfo, error) {
	var out keyInfo
	if err := c.do(ctx, http.MethodPost, "/v2/CreateKey", map[string]string{"name": name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// createExpiringKey creates a key that Garage stops honouring at expiresAt.
//
// The expiry is set at creation rather than afterwards on purpose: a key that
// exists for a moment without one is a key that outlives the process if that
// process dies between the two calls, and these are minted per request by a
// service that hands the secret to a caller and forgets it. There is nothing to
// come back and clean up with.
func (c *adminClient) createExpiringKey(ctx context.Context, name string, expiresAt time.Time) (*keyInfo, error) {
	body := map[string]any{
		"name":       name,
		"expiration": expiresAt.UTC().Format(time.RFC3339),
	}
	var out keyInfo
	if err := c.do(ctx, http.MethodPost, "/v2/CreateKey", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// listKeys returns every access key in the cluster, with the expiry state Garage
// keeps for each.
func (c *adminClient) listKeys(ctx context.Context) ([]keyListItem, error) {
	var out []keyListItem
	if err := c.do(ctx, http.MethodGet, "/v2/ListKeys", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// keyExists reports whether an access key with the given id exists.
func (c *adminClient) keyExists(ctx context.Context, accessKeyID string) (bool, error) {
	err := c.do(ctx, http.MethodGet, "/v2/GetKeyInfo?id="+url.QueryEscape(accessKeyID), nil, nil)
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

// setKeyExpiration gives an access key a moment at which Garage itself stops
// honouring it.
//
// This is what makes a rotation overlap safe to promise. The alternative — the
// controller coming back to delete the old key when the window is over — is a
// promise only as good as the controller: a pod that is evicted, a reconcile that
// errors, a cluster that is busy, and the replaced key stays valid indefinitely.
// An expiry is held by the backend, so the window closes on time whether or not
// anything is still watching it.
//
// The key is not deleted by this: an expired key still exists in Garage's
// metadata (ListKeys reports expired: true), and removing it is a later,
// unhurried cleanup.
func (c *adminClient) setKeyExpiration(ctx context.Context, accessKeyID string, at time.Time) error {
	body := map[string]any{"expiration": at.UTC().Format(time.RFC3339)}
	err := c.do(ctx, http.MethodPost, "/v2/UpdateKey?id="+url.QueryEscape(accessKeyID), body, nil)
	if isNotFound(err) {
		return nil // already gone: nothing left to expire
	}
	return err
}

// deleteKey removes an access key (idempotent: a missing key is not an error).
func (c *adminClient) deleteKey(ctx context.Context, accessKeyID string) error {
	err := c.do(ctx, http.MethodPost, "/v2/DeleteKey?id="+url.QueryEscape(accessKeyID), nil, nil)
	if isNotFound(err) {
		return nil
	}
	return err
}

// getBucketByAlias returns the bucket with the given global alias, or (nil,
// false, nil) when it does not exist.
func (c *adminClient) getBucketByAlias(ctx context.Context, alias string) (*bucketInfo, bool, error) {
	var out bucketInfo
	err := c.do(ctx, http.MethodGet, "/v2/GetBucketInfo?globalAlias="+url.QueryEscape(alias), nil, &out)
	if err == nil {
		return &out, true, nil
	}
	if isNotFound(err) {
		return nil, false, nil
	}
	return nil, false, err
}

// bucketListItem is one entry of GET /v2/ListBuckets.
type bucketListItem struct {
	ID            string   `json:"id"`
	GlobalAliases []string `json:"globalAliases"`
}

// cleanupIncompleteUploads removes the multipart uploads of a bucket that were
// started more than olderThan ago and never finished, and reports how many went.
//
// Garage does not do this on its own: an upload interrupted halfway leaves its
// parts in the bucket for good. They are not objects — they do not appear in a
// listing, they are not counted against the bucket's quota, and the only place
// they show up is unfinishedMultipartUploadBytes in GetBucketInfo — but they
// occupy the volume exactly like data that is being used.
func (c *adminClient) cleanupIncompleteUploads(ctx context.Context, bucketID string, olderThan time.Duration) (int64, error) {
	body := map[string]any{
		"bucketId":      bucketID,
		"olderThanSecs": int64(olderThan.Seconds()),
	}
	var out struct {
		UploadsDeleted int64 `json:"uploadsDeleted"`
	}
	if err := c.do(ctx, http.MethodPost, "/v2/CleanupIncompleteUploads", body, &out); err != nil {
		return 0, err
	}
	return out.UploadsDeleted, nil
}

// listBuckets returns every bucket of the cluster.
//
// The migration copies what a cluster HOLDS rather than what the module believes
// it should hold: a bucket whose ownership record was lost is still a bucket full
// of a consumer's data, and leaving it behind because a ConfigMap forgot about it
// is not a migration.
func (c *adminClient) listBuckets(ctx context.Context) ([]bucketListItem, error) {
	var out []bucketListItem
	if err := c.do(ctx, http.MethodGet, "/v2/ListBuckets", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// getBucketByID reads a bucket by its identifier.
func (c *adminClient) getBucketByID(ctx context.Context, id string) (*bucketInfo, error) {
	var out bucketInfo
	if err := c.do(ctx, http.MethodGet, "/v2/GetBucketInfo?id="+url.QueryEscape(id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// createBucket creates a bucket with the given global alias.
func (c *adminClient) createBucket(ctx context.Context, alias string) (*bucketInfo, error) {
	var out bucketInfo
	if err := c.do(ctx, http.MethodPost, "/v2/CreateBucket", map[string]string{"globalAlias": alias}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// setBucketQuota sets the bucket's size quota in bytes, in place, leaving the
// object-count quota alone.
//
// Garage enforces it on write: a PUT that would take the bucket past maxSize is
// refused with QuotaExceeded. That is the whole point of carrying spec.size into
// the backend rather than only into the volume sizing — without it a single
// consumer could fill the store and every other platform component would start
// failing writes for a reason none of them could see.
//
// maxObjects is deliberately sent as null rather than omitted: UpdateBucket
// replaces the quota object, so leaving the field out would be read as "no object
// quota" anyway, and saying it explicitly is how the intent survives a reader.
func (c *adminClient) setBucketQuota(ctx context.Context, bucketID string, maxSize int64) error {
	body := map[string]any{
		"quotas": map[string]any{
			"maxSize":    maxSize,
			"maxObjects": nil,
		},
	}
	return c.do(ctx, http.MethodPost, "/v2/UpdateBucket?id="+url.QueryEscape(bucketID), body, nil)
}

// deleteBucket removes a bucket by id (idempotent).
func (c *adminClient) deleteBucket(ctx context.Context, bucketID string) error {
	err := c.do(ctx, http.MethodPost, "/v2/DeleteBucket?id="+url.QueryEscape(bucketID), nil, nil)
	if isNotFound(err) {
		return nil
	}
	return err
}

// deny revokes the given permissions from a key on a bucket (idempotent). The
// permissions set to true are the ones REMOVED.
func (c *adminClient) deny(ctx context.Context, bucketID, accessKeyID string, perms permissions) error {
	body := map[string]any{
		"bucketId":    bucketID,
		"accessKeyId": accessKeyID,
		"permissions": perms,
	}
	return c.do(ctx, http.MethodPost, "/v2/DenyBucketKey", body, nil)
}

// allow grants the given permissions to a key on a bucket (idempotent).
func (c *adminClient) allow(ctx context.Context, bucketID, accessKeyID string, perms permissions) error {
	body := map[string]any{
		"bucketId":    bucketID,
		"accessKeyId": accessKeyID,
		"permissions": perms,
	}
	return c.do(ctx, http.MethodPost, "/v2/AllowBucketKey", body, nil)
}
