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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// temporaryKeyPrefix marks a key minted for one caller of the credentials
// endpoint, as opposed to the long-lived key a SystemBucket owns.
//
// It is the handle the sweeper works by, and it is in the NAME rather than in a
// list the controller keeps: Garage is the only place these keys exist, the
// process that minted them does not remember them, and a record kept anywhere
// else would be a record that can disagree with the backend.
const temporaryKeyPrefix = "sts-"

// maxLiveTemporaryKeysPerBucket bounds how many unexpired temporary keys one
// bucket can have at once.
//
// Nothing about the credentials endpoint costs a caller anything: it presents a
// token it already has and gets a key. A client that asks on every request, or
// one wedged in a retry loop, mints a key per attempt — each one a record in the
// metadata Garage replicates to every node, each one alive for up to twelve
// hours whatever the caller does next. The sweeper only removes keys that have
// already expired, so it cannot bound the rate.
//
// Fifty is far above any sane use: a client that renews before its hour is out
// holds one or two. It is low enough that a loop is stopped in minutes rather
// than after it has filled the key table.
const maxLiveTemporaryKeysPerBucket = 50

// TemporaryCredentials is one short-lived key pair, as handed to a caller.
type TemporaryCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	ExpiresAt       time.Time
}

// IssueTemporaryAccess mints a key that Garage stops honouring at expiresAt and
// grants it on the bucket.
//
// The lifetime is enforced by the backend, which is what makes this safe to hand
// out: nothing has to come back and revoke it. The sweeper below removes the
// expired records afterwards, and that is housekeeping — a key it misses is a
// dead key, not a live one.
//
// write=false grants read only. There is no owner permission: a temporary key
// must not be able to delete the bucket it was given access to.
func (d *Driver) IssueTemporaryAccess(
	ctx context.Context, store *backend.Store, bucketName, subject string, write bool, expiresAt time.Time,
) (TemporaryCredentials, error) {
	store, migration, err := d.servingPlane(ctx, store)
	if err != nil {
		return TemporaryCredentials{}, err
	}
	if write && migration.writesFrozen() {
		// The freeze is not a suggestion. A key minted with write access during the
		// cutover would write into the cluster whose contents are being compared
		// with the new one, and the difference would be copied over or lost.
		// Refusing is the honest answer: for the length of the switch, this store
		// takes no writes from anybody.
		return TemporaryCredentials{}, fmt.Errorf(
			"the system object storage is switching to a new Garage cluster and is refusing writes; " +
				"ask again in a few minutes, or ask for read-only credentials")
	}
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil {
		return TemporaryCredentials{}, err
	}
	if !ready {
		return TemporaryCredentials{}, fmt.Errorf("the system object storage is not provisioned yet")
	}

	b, found, err := svc.getBucketByAlias(ctx, bucketName)
	if err != nil {
		return TemporaryCredentials{}, fmt.Errorf("look up bucket %q: %w", bucketName, err)
	}
	if !found {
		return TemporaryCredentials{}, fmt.Errorf("bucket %q does not exist", bucketName)
	}

	live, err := d.liveTemporaryKeys(ctx, svc, b)
	if err != nil {
		return TemporaryCredentials{}, err
	}
	if live >= maxLiveTemporaryKeysPerBucket {
		// Refused, not queued and not silently granted. The caller is asking faster
		// than its credentials expire, which is a client that does not reuse what it
		// is given — and answering it would go on costing the cluster a key record
		// per request until somebody noticed.
		d.log.Error("too many live temporary credentials for one bucket; refusing to issue more",
			"bucket", bucketName, "subject", subject, "live", live, "limit", maxLiveTemporaryKeysPerBucket)
		return TemporaryCredentials{}, fmt.Errorf(
			"bucket %q already has %d unexpired temporary credentials, which is the limit: "+
				"reuse the credentials you were given until they expire instead of asking for new ones",
			bucketName, live)
	}

	key, err := svc.createExpiringKey(ctx, temporaryKeyName(bucketName, subject), expiresAt)
	if err != nil {
		return TemporaryCredentials{}, fmt.Errorf("create a temporary access key: %w", err)
	}

	if err := svc.allow(ctx, b.ID, key.AccessKeyID, permissions{Read: true, Write: write}); err != nil {
		// The key exists and grants nothing; it would expire on its own, but leaving
		// it is still leaving a record nobody asked for.
		_ = svc.deleteKey(ctx, key.AccessKeyID)
		return TemporaryCredentials{}, fmt.Errorf("grant the temporary key on bucket %q: %w", bucketName, err)
	}

	return TemporaryCredentials{
		AccessKeyID:     key.AccessKeyID,
		SecretAccessKey: key.SecretAccessKey,
		ExpiresAt:       expiresAt,
	}, nil
}

// temporaryKeyName is the display name a temporary key carries in Garage: the
// prefix the sweeper selects by, the bucket, and who asked for it.
//
// The subject is in there because this name is the only audit trail these keys
// have — `garage key list` is where an operator looks when a bucket is being
// written to by something they did not expect, and "sts-loki-…" answers a
// different question than "sts-d8-monitoring:loki-agent-…".
func temporaryKeyName(bucketName, subject string) string {
	return fmt.Sprintf("%s%s-%s", temporaryKeyPrefix, bucketName, sanitiseSubject(subject))
}

// sanitiseSubject keeps a ServiceAccount's identity readable in a key name
// without letting it carry anything that would make the name ambiguous.
func sanitiseSubject(subject string) string {
	subject = strings.TrimPrefix(subject, "system:serviceaccount:")
	subject = strings.ReplaceAll(subject, ":", ".")
	if len(subject) > 64 {
		subject = subject[:64]
	}
	return subject
}

// SweepExpiredTemporaryAccess deletes the temporary keys Garage has already
// stopped honouring, and reports how many went.
//
// It is not what makes the credentials expire — the backend does that on its own
// clock, which is the point of setting the expiry at creation. This is the
// bookkeeping behind it: a cluster issuing credentials every few minutes
// accumulates a key record per request, and the metadata they sit in is
// replicated to every node.
//
// Only keys this module minted are considered, by name prefix, and only the ones
// Garage itself reports as expired. Nothing here decides that a key is old
// enough to remove.
func (d *Driver) SweepExpiredTemporaryAccess(ctx context.Context, store *backend.Store) (int, error) {
	store, _, err := d.servingPlane(ctx, store)
	if err != nil {
		return 0, err
	}
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil || !ready {
		return 0, err
	}

	keys, err := svc.listKeys(ctx)
	if err != nil {
		return 0, fmt.Errorf("list access keys: %w", err)
	}

	swept := 0
	for _, k := range keys {
		if !strings.HasPrefix(k.Name, temporaryKeyPrefix) || !k.Expired {
			continue
		}
		if err := svc.deleteKey(ctx, k.ID); err != nil {
			// One key that will not go must not stop the rest: the next pass tries
			// again, and the key is already powerless either way.
			d.log.Error("unable to delete an expired temporary access key",
				"accessKeyID", k.ID, "name", k.Name, "error", err.Error())
			continue
		}
		swept++
	}
	return swept, nil
}

// liveTemporaryKeys counts the unexpired temporary keys granted on one bucket.
//
// From the bucket's own key list, not from the key names. The name of a
// temporary key is "sts-<bucket>-<subject>" and both halves can contain a
// hyphen, so counting by prefix charges "loki-staging"'s keys to "loki" and
// refuses credentials to a consumer that has asked for none. A test found that
// before a cluster did.
//
// The bucket says which keys it granted; the key list says which of them Garage
// still honours. Expired ones do not count — they are dead credentials waiting
// for the sweeper, and refusing a caller because of them would turn a
// housekeeping delay into an outage.
func (d *Driver) liveTemporaryKeys(ctx context.Context, svc *adminClient, bucket *bucketInfo) (int, error) {
	granted := make(map[string]struct{}, len(bucket.Keys))
	for _, k := range bucket.Keys {
		if strings.HasPrefix(k.Name, temporaryKeyPrefix) {
			granted[k.AccessKeyID] = struct{}{}
		}
	}
	if len(granted) == 0 {
		return 0, nil
	}

	keys, err := svc.listKeys(ctx)
	if err != nil {
		return 0, fmt.Errorf("count the temporary credentials of bucket %q: %w", bucketAlias(bucket), err)
	}
	live := 0
	for _, k := range keys {
		if k.Expired {
			continue
		}
		if _, ok := granted[k.ID]; ok {
			live++
		}
	}
	return live, nil
}

// bucketAlias is the bucket's global name, for a message. A bucket always has
// one here — this module creates them by alias — but a bucket without one must
// not turn a refusal into a panic.
func bucketAlias(bucket *bucketInfo) string {
	if len(bucket.GlobalAliases) == 0 {
		return bucket.ID
	}
	return bucket.GlobalAliases[0]
}

// incompleteUploadAge is how old an unfinished multipart upload has to be before
// the module removes it.
//
// A day, because the only thing that can tell an abandoned upload from a slow one
// is time, and the module has no way to ask the consumer. A platform component
// uploading for more than twenty-four hours without finishing has a problem this
// sweep is not going to make worse; one that was interrupted — by a cutover, by
// its own restart — left parts nobody is coming back for.
const incompleteUploadAge = 24 * time.Hour

// SweepIncompleteUploads removes the parts of multipart uploads that were started
// and never finished, across every bucket of the store, and reports how many
// uploads went.
//
// Garage does not do this on its own, and what is left behind is invisible in the
// ways that matter: the parts are not objects, so they are in no listing and are
// not counted against the bucket's quota — but they occupy the volume exactly
// like data in use. A consumer whose uploads are interrupted often enough fills
// the data plane with storage that nothing reports and nobody can find.
func (d *Driver) SweepIncompleteUploads(ctx context.Context, store *backend.Store) (int, error) {
	store, _, err := d.servingPlane(ctx, store)
	if err != nil {
		return 0, err
	}
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil || !ready {
		return 0, err
	}

	buckets, err := svc.listBuckets(ctx)
	if err != nil {
		return 0, fmt.Errorf("list the buckets: %w", err)
	}

	swept := 0
	for _, b := range buckets {
		deleted, err := svc.cleanupIncompleteUploads(ctx, b.ID, incompleteUploadAge)
		if err != nil {
			// One bucket's failure is not the others': this is housekeeping, and the
			// next pass tries again in ten minutes.
			d.log.Error("unable to clean up unfinished uploads", logger.Err(err), "bucket", b.ID)
			continue
		}
		swept += int(deleted)
	}
	return swept, nil
}
