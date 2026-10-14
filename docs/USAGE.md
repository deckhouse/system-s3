---
title: "Usage"
weight: 40
---

## Contract for module developers

1. Declare a `SystemBucket` in your module's namespace labelled `heritage: deckhouse`. `spec.size` is a bucket quota, not a dedicated PVC. Deleting the CR destroys its objects: handle this intentionally in your module's lifecycle.
2. Wait for `Ready=True` with `observedGeneration` matching `metadata.generation`, then read the ConfigMap in `status.connectionRef`. Do not hard-code endpoints or assume the ConfigMap exists immediately.
3. Grant your consumer ServiceAccount `systembuckets/read` and, for writers, `systembuckets/write`, restricted by `resourceNames`. Owning a bucket CR does not grant the consumer access; STS checks TokenReview and SubjectAccessReview.
4. Mount a projected token with audience `system-s3-sts`, both CAs, and WebIdentity settings from the ConfigMap. Re-read the token file on refresh because kubelet rotates it. Its one-hour example lifetime is independent of the S3 key's maximum thirty minutes.
5. Use a caching SDK credentials provider and the response's actual `Expiration`. Do not request a key per S3 operation, store it in a Secret, or log the token/STS response. SDK requests for an hour are clamped to thirty minutes. Test refresh with one process running longer than thirty minutes.
6. Handle outages during HA changes and early key invalidation after StorageClass cutover. Expiry-based refresh alone does not establish recovery: test reacquisition in your SDK. A key is checked when a request starts: an upload that began before its key expired runs to the end, and a multipart upload started with one key can be continued and completed with the next one, since the upload belongs to the bucket, not to the key. A multipart upload that nobody completes needs a retry or an abort; its parts are removed after 24 hours. A `403` may also indicate quota exhaustion or missing permissions; endlessly issuing new keys is not a general solution.
7. Set request timeouts in the S3 client and retry on connection errors. When a replica of a highly available store restarts, the other replicas notice it is gone only after about 75 seconds of missed pings (a threshold fixed in Garage). Until then a request may wait on that replica, and a long download already under way may be cut off with a connection reset rather than carried on from another copy: on a load test, two of six 32 MiB downloads through a replica restart failed about 90 seconds in. Retry such a GET with a `Range` from the last byte received instead of starting over; a pooled connection to the restarted pod can also hang until the client's own timeout, so do not leave it unbounded.

### Usage and quota reporting

`kubectl get sb` displays `Objects`, `UsedBytes`, and `TotalBytes`; `-o wide` also displays `ObjectLimit`. `TotalBytes` is this bucket's applied quota, not Garage's physical capacity; `Size` is its desired quota. Empty buckets explicitly report `objects: 0` and `usedBytes: 0`.

```shell
kubectl -n d8-monitoring get sb loki -o wide
kubectl -n d8-monitoring get sb loki -o json |
  jq '.status | {objects,usedBytes,quotaBytes,quotaObjects,conditions}'
```

Counters update during reconciliation, not in real time, and may retain the last successful reading while the backend is unavailable. Raise `spec.size` and wait for `SizeUpToDate` for that generation; shrinking is prohibited. Test byte and object-count quotas separately.

## Asking for a bucket

A Deckhouse component creates a `SystemBucket` in its own namespace:

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: SystemBucket
metadata:
  name: loki
  namespace: d8-monitoring
spec:
  size: 10Gi
```

`size` is how much the bucket asks for. The default (`1Gi`) fits a component that stores a little; ask for more explicitly when you need it. It is also what the system storage sizes itself from — it provisions the sum of every `SystemBucket`'s size — so raising it here is what makes the volumes grow. A write past `size` is refused by the backend.

`size` also caps the number of objects: `size / 64 KiB` (16384 for `1Gi`, shown in `status.quotaObjects`). Every object costs disk space beyond its bytes — about 2 KiB of metadata and rounding to the filesystem block — and the store's volumes allow for that overhead up to this count. A write that would exceed it is refused with 403 `AccessDenied` ("Object quota is reached") even while the bucket holds less than `size`. A component that stores many objects smaller than 64 KiB on average should ask for a `size` that fits their count, not only their bytes.

`size` can be raised at any time and cannot be lowered — the API server refuses it. `SystemBucket.SizeUpToDate=True` means the backend enforces **this bucket's** quota; `QuotaPending` means it has not applied it yet. This does not guarantee sufficient physical capacity. Shared PVC growth and CSI failures are reported only in `Module/system-s3.CapacityReady`: `Growing` while volumes grow, `CannotGrow` when expansion is impossible or fails. When physical capacity is exhausted, writes may fail before the quota is reached: check both resources. The bucket may remain `Ready` during PVC expansion.

```shell
kubectl -n d8-monitoring get sb loki -o jsonpath='{.status.conditions[?(@.type=="SizeUpToDate")]}'
```

The controller creates the bucket in the system storage and writes a `ConfigMap` next to the resource with everything needed to reach it:

```console
$ kubectl -n d8-monitoring get sb
NAME   BUCKET               PHASE   CONNECTION           READY   AGE
loki   d8-monitoring-loki   Ready   loki-s3-connection   True    30s
```

| Key | Value |
|-----|-------|
| `S3_ENDPOINT` | In-cluster endpoint of the system storage, `https://`. |
| `S3_REGION` | Region the endpoint advertises (`garage`). |
| `S3_BUCKET` | Effective bucket name. |
| `S3_CA_CERT` | CA that signs the S3 endpoint. The module's own, like `S3_STS_CA_CERT` (today it is the same CA), so nothing in the cluster trusts it by default. |
| `S3_STS_ENDPOINT` | Where to exchange a ServiceAccount token for credentials. |
| `S3_STS_AUDIENCE` | The `audience` to request for the ServiceAccount token the pod presents at `S3_STS_ENDPOINT` — always `system-s3-sts`. It goes into the pod spec, in the projected token volume (see below): Kubernetes cannot take it from a ConfigMap. The endpoint accepts only tokens issued for this audience, so the pod's default token is refused, and a token issued for it does not authenticate to the API server — if leaked, it can only be used to ask for the credentials its ServiceAccount is allowed anyway. |
| `S3_STS_CA_CERT` | CA that signs the credentials endpoint. It is the module's own, so nothing in the cluster trusts it by default. |
| `AWS_ROLE_ARN`, `AWS_ENDPOINT_URL_STS`, `AWS_ENDPOINT_URL_S3`, `AWS_REGION` | The same connection under the names the AWS SDKs read on their own: the bucket as `arn:deckhouse:system-s3:::systembucket/<namespace>/<name>`, the credentials endpoint, the S3 endpoint and the region. See [AWS SDKs without code](#aws-sdks-without-code). |

**There are no credentials in it, and none anywhere else.** The module issues no
long-lived key for a bucket — not for its owner either. Every consumer, including
the component that asked for the bucket, gets credentials the same way: it
presents its ServiceAccount token at the credentials endpoint and receives a key
pair the backend stops honouring at most thirty minutes later.

`envFrom` this ConfigMap for the addresses, mount the token, and mount the two
CAs into a directory of their own:

```yaml
    envFrom:
      - configMapRef:
          name: loki-s3-connection
    env:
      # Go: the system roots plus the module's CAs, for every TLS client in the
      # process. /etc/ssl/certs stays in the list because SSL_CERT_DIR replaces
      # the default directories rather than adding to them.
      - name: SSL_CERT_DIR
        value: /etc/ssl/certs:/etc/system-s3/ca
    volumeMounts:
      - name: s3-token
        mountPath: /var/run/secrets/system-s3
        readOnly: true
      - name: s3-ca
        mountPath: /etc/system-s3/ca
        readOnly: true
    volumes:
      - name: s3-token
        projected:
          sources:
            - serviceAccountToken:
                path: token
                audience: system-s3-sts     # S3_STS_AUDIENCE
                expirationSeconds: 3600
      - name: s3-ca
        configMap:
          name: loki-s3-connection
          items:                            # the CAs alone, as PEM files
            - {key: S3_CA_CERT, path: s3-ca.crt}
            - {key: S3_STS_CA_CERT, path: sts-ca.crt}
```

**The S3 endpoint is TLS-only**, with a certificate from the module's own CA, and
the client has to trust that CA — there is no plaintext port to fall back to.

- **Go** (minio-go, aws-sdk-go-v2, anything on `crypto/tls`): the example above is
  all it takes. Go on Linux reads its system roots from the bundle file *and*
  from every file in the `SSL_CERT_DIR` directories, so the module's CAs are
  added to the public ones, and every client in the process that uses the
  system roots trusts both endpoints without a line of code. Go reads the
  system roots once per process: a new CA takes a restart. The CA is long-lived;
  renewing the server certificate does not change it.
- **AWS CLI and boto3**: `AWS_CA_BUNDLE=/etc/system-s3/ca/s3-ca.crt`.
  It replaces the SDK's bundle rather than adding to it, so a client that also
  talks to public endpoints needs a bundle with both.
- **OpenSSL-based tools** (curl, Python `ssl`): `SSL_CERT_DIR` does not help —
  OpenSSL looks certificates up in a directory by hashed file names, which a
  ConfigMap mount does not have. Point the tool at the file:
  `curl --cacert /etc/system-s3/ca/s3-ca.crt`, `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`.
- **Not `subPath` into `/etc/ssl/certs`.** A `subPath` mount is not updated when
  the ConfigMap changes, so the pod would keep a CA the module has replaced.

## Getting credentials

The exchange is `AssumeRoleWithWebIdentity`, which every S3 client library
implements:

```go
creds, _ := credentials.NewSTSWebIdentity(
    stsEndpoint+"/d8-monitoring/loki",              // S3_STS_ENDPOINT + /<namespace>/<name>
    func() (*credentials.WebIdentityToken, error) {
        token, err := os.ReadFile("/var/run/secrets/system-s3/token")
        return &credentials.WebIdentityToken{Token: string(token)}, err
    },
)
```

The endpoint returns a key pair that Garage stops honouring at the expiry in the
response (30 minutes by default, `DurationSeconds` between 15 and 30 minutes; larger SDK requests are clamped to 30 minutes).
There is no session token: the credentials are a real key pair, and S3 requests
signed with them need nothing else.

### AWS SDKs without code

The AWS CLI, boto3, aws-sdk-go-v2 and the other AWS SDKs implement this exchange
themselves: given a role, a token file and an STS endpoint, they ask for a key
before the first request and ask again before it expires. The ConfigMap already
carries the role and both endpoints under the SDKs' names, so a consumer adds
only the two paths that belong to its pod:

```yaml
    envFrom:
      - configMapRef:
          name: loki-s3-connection
    env:
      - name: AWS_WEB_IDENTITY_TOKEN_FILE
        value: /var/run/secrets/system-s3/token
      - name: AWS_CA_BUNDLE               # one CA signs both endpoints today
        value: /etc/system-s3/ca/s3-ca.crt
```

with the same `s3-token` and `s3-ca` volumes as above. `aws s3 cp` then works
without `--endpoint-url` and without keys in the environment. The four `AWS_*`
keys are in the ConfigMap only while the module serves the credentials endpoint.

**Being allowed to ask is an RBAC grant**, and the component that created the
`SystemBucket` needs it like anyone else — owning the resource is permission to
edit the resource, not to read the objects in its bucket:

```yaml
# Two verbs, two subresources: reading the objects in a bucket and editing the
# SystemBucket resource are different permissions.
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: loki-bucket-access
  namespace: d8-monitoring
rules:
  - apiGroups: ["storage.deckhouse.io"]
    resources: ["systembuckets/read", "systembuckets/write"]
    resourceNames: ["loki"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: loki-bucket-access
  namespace: d8-monitoring
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: loki-bucket-access}
subjects:
  - kind: ServiceAccount
    name: loki
    namespace: d8-monitoring
```

A second component that needs the same bucket gets its own grant, with a subject
in its own namespace. Nothing is copied between them, and revoking access is
deleting the `RoleBinding`: what was handed out expires on its own, and the next
request is refused.

Who holds access to a bucket right now is in the metric
`system_s3_temporary_key_expiration_seconds`: one series per unexpired temporary key, labelled
`bucket`, `access_key_id`, `subject` (the ServiceAccount the key was issued to) and `write`, valued
with the Unix time it expires. The list is refreshed every minute; the secret half of a key is not in it.

**Reuse the key you are given until it expires.** Every request mints a new key,
and that key stays alive for the rest of its lifetime (30 minutes by default)
whether the client still wants it or not, in metadata Garage replicates to every
node. One bucket is therefore allowed at most 50 unexpired temporary keys at a
time: a client that asks for credentials on every request reaches that limit and
is refused, with the reason. A well-behaved client holds one key and renews it
near the end of its lifetime.

## Bucket names

`spec.bucketName` is optional. When it is omitted the bucket is named `<namespace>-<name>`, because the backend's bucket names are flat while `SystemBucket` is namespaced: two components asking for `backups` must not end up sharing one bucket.

Set it explicitly only when something outside the cluster needs a specific name. Two `SystemBucket` resources may not map to the same bucket name: the admission webhook rejects the second one, and if it is bypassed — two simultaneous creations, or a cluster where the webhook is not installed — the controller refuses it as well, reporting `BucketReady=False` with the owner it belongs to. Give that resource a different `spec.bucketName`.

The refusal is not caution for its own sake: a `SystemBucket` owns its data outright and there is no reclaim policy, so two owners of one bucket means both write to the same storage and deleting either one destroys the other's objects.

Whichever way the name is arrived at, it has to be one S3 accepts: 3 to 63 characters, lower-case letters, digits and dashes, starting and ending with a letter or a digit. The derived form is where that bites, because it is composed rather than typed — a namespace and a resource name of up to 63 characters each add up to as much as 127. Set `spec.bucketName` when the pair is too long:

```console
$ kubectl -n d8-observability-long-term-metrics apply -f systembucket.yaml
Error from server: admission webhook denied the request: the bucket name this
SystemBucket maps to,
"d8-observability-long-term-metrics-prometheus-remote-write-backend", cannot be
used: it is 66 characters long, and S3 bucket names stop at 63. With
spec.bucketName unset the name is derived as <namespace>-<name> …
```

The controller refuses the same name for the same reason, so a resource created while the webhook was down reports `BucketReady=False` saying what to set instead of leaving the backend to answer.

## Deleting a bucket

```shell
kubectl -n d8-monitoring delete systembucket loki
```

{{< alert level="danger" >}}
This deletes the bucket and every object in it. There is no reclaim policy and no recycle bin — make sure the data is no longer needed, or copied elsewhere, first.
{{< /alert >}}

The resource keeps a finalizer until the bucket is gone from the backend, so a failure to reach the storage delays the deletion instead of orphaning data in it.

## Status and troubleshooting

`SystemBucket.status.conditions` reports the stages in order:

| Condition | Meaning when `False` |
|-----------|----------------------|
| `BucketReady` | The bucket could not be created, or its dependency is unavailable (`WaitingForDependency`; details in `Module/system-s3`). |
| `ConnectionReady` | The `ConfigMap` with the connection parameters could not be written. |
| `Ready` | Aggregate of the above. |

A `SystemBucket` in a namespace without the `heritage: deckhouse` label is rejected by the webhook. If admission was bypassed and the resource exists anyway, the controller refuses to provision it with `BucketReady=False`, reason `ReconcileFailed`, and a message explaining the namespace label. An unavailable webhook also rejects requests because its `failurePolicy` is `Fail`.

Shared Garage state is published only in `Module/system-s3.status.conditions`, including before the first bucket exists. The system-s3 controller does not change Deckhouse conditions (`IsReady`, enablement conditions, etc.) or `Module.status.phase`.

| Condition | Meaning |
|-----------|---------|
| `StorageReady` | Bootstrap, Garage health, quorum and layout. |
| `PlacementReady` | Replica placement, eligible-node shortages, relocation and saved node identities. |
| `CapacityReady` | Shared PVC capacity, expansion and CSI failures. |
| `ReplicationReady` | HA mode convergence: `ResizeFreeze`, `ResizeStop`, `ResizePurge`, `ResizeSync`. |
| `MigrationReady` | StorageClass migration: `Provisioning`, `Copying`, `Verifying`, `Cutover`, `Draining`, `Decommission`. |

`False` can mean a normal operation in progress, not only a failure; inspect `reason` and `message`. `Unknown/NotEvaluated` means this reconciliation did not reach the check. Completed operations return to `True`. Conditions show current state, not an event history. On upgrade the controller removes legacy Garage conditions from buckets; replace waits for `SystemBucket.StorageReady` with `Module.StorageReady`.

The quota condition was renamed from `SizeApplied` to `SizeUpToDate`. Reconciliation removes the old name, including when Garage is unavailable. Update consumer JSONPath expressions and readiness checks. The Prometheus metric name `system_s3_bucket_size_applied` is retained for compatibility.

The data plane itself is described by the controller's metrics, refreshed on every reconciliation pass (every 30 seconds by default). The console draws the storage page from them, not from the pods and StatefulSets in `d8-system-s3`:

| Metric | Labels | Value |
|--------|--------|-------|
| `system_s3_replica_ready` | `plane`, `replica`, `node`, `serving` | `1` if the Garage pod is `Ready`, `0` otherwise. One series per existing pod; `node` is empty while the pod is not scheduled. |
| `system_s3_store_info` | `storage_class`, `high_availability`, `replicas` | `1`. The shape the serving cluster actually runs in: the StorageClass its volumes are bound to, the mode of its pinned replication factor and the replica count. Differs from the module settings while a change is pending. |
| `system_s3_plane_info` | `plane`, `storage_class`, `high_availability`, `replicas`, `serving` | `1` for each existing Garage cluster. |

`plane` is the number of the Garage cluster; during a migration there are two, and `serving="true"` marks the one the endpoint points at. Pods and clusters that no longer exist lose their series. Only the controller replica holding the leader lease publishes them.

To inspect the storage itself:

```shell
kubectl get module system-s3 -o json | jq '.status.conditions'
kubectl -n d8-system-s3 get pods,sts,pvc
kubectl -n d8-system-s3 logs deploy/controller -c controller
```

### Garage metrics and alerts

Prometheus scrapes `/metrics` of every Garage pod, of every data plane, as job `system-s3-garage`. Each series carries `pod`, `node`, `store` and `incarnation`; during a StorageClass migration both planes are scraped and told apart by `incarnation`. The scrape uses a token of its own, from the `garage-metrics-token` Secret in `d8-system-s3`: it opens `/metrics` and nothing else, and Prometheus never holds the admin token. The controller creates the Secret before the first Garage pod and never rewrites it; Garage reads the token when it starts.

The **System S3 Garage** dashboard shows S3 requests, errors and p95/p99 latency by operation, block read and write throughput and duration, the resync queue, free space per replica, and the cluster's health as its nodes see it.

| Alert | Fires when |
|-------|------------|
| `D8SystemS3GarageClusterUnavailable` | No node of a plane has a quorum for every partition, for 1 minute: reads and writes fail. |
| `D8SystemS3GarageClusterDegraded` | A storage node is disconnected but the plane still serves, for 5 minutes. A single-replica store never reports this state. |
| `D8SystemS3GarageNodeDisconnected` | A node in the layout has not been seen for more than 5 minutes. |
| `D8SystemS3GarageServerErrors` | More than ten S3 `5xx` responses per 5 minutes. |
| `D8SystemS3GarageResyncErrors` | A replica has data blocks it failed to resync for 15 minutes. |
| `D8SystemS3GarageDiskAlmostFull` | A replica's volume has less than 10% free for 10 minutes. |
| `D8SystemS3GarageMetricsUnavailable` | A Garage pod could not be scraped for 15 minutes; the alerts above do not see it. |

Each alert's description says what to check and what to do.

## Disabling the module, and destroying the cluster

Disabling the module is guarded: Deckhouse refuses `enabled: false` until the `ModuleConfig` carries the acknowledgement, because the data plane and every object stored in it go with it.

```shell
kubectl annotate moduleconfig system-s3 modules.deckhouse.io/allow-disabling=true
kubectl patch moduleconfig system-s3 --type merge -p '{"spec":{"enabled":false}}'
```

What is left behind afterwards, and what is not:

- the `SystemBucket` resources stay, frozen at their last status, and so do their CRDs — disabling a module does not delete either. The module's own finalizers are released on the way out, so they can be deleted by hand;
- the data plane's `PersistentVolumeClaim`s go with the namespace, and what happens to the volumes behind them is the `StorageClass`'s reclaim policy to decide: `Delete` removes them with everything stored in them, `Retain` keeps them. The module deliberately does not interfere — it no longer owns a volume layer of its own, and overriding somebody else's policy on the way out is not its call;
- the webhook configurations are removed, because an admission webhook whose backend is gone would block work with the resources it guards.

{{< alert level="warning" >}}
**Destroying a cluster requires disabling the module first.** `dhctl destroy` deletes every `PersistentVolumeClaim` and waits for them to go; the data plane's `StatefulSet` re-creates its claims for as long as it exists, so the step times out and the cluster is left half-removed. Disabling the module takes the `StatefulSet` away, and the destroy then completes.
{{< /alert >}}

{{< alert level="info" >}}
While the module is at the `Experimental` stage, enabling it again after a disable needs the platform's permission: add `system-s3` to `spec.settings.allowedExperimentalModules` in the `deckhouse` `ModuleConfig` (or set `allowExperimentalModules: true`) before setting `enabled: true`.
{{< /alert >}}
