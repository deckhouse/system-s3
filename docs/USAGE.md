---
title: "Usage"
weight: 40
---

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

The controller creates the bucket in the system storage and writes a `ConfigMap` next to the resource with everything needed to reach it:

```console
$ kubectl -n d8-monitoring get sb
NAME   BUCKET               PHASE   CONNECTION           READY   AGE
loki   d8-monitoring-loki   Ready   loki-s3-connection   True    30s
```

| Key | Value |
|-----|-------|
| `S3_ENDPOINT` | In-cluster endpoint of the system storage. |
| `S3_REGION` | Region the endpoint advertises (`garage`). |
| `S3_BUCKET` | Effective bucket name. |
| `S3_STS_ENDPOINT` | Where to exchange a ServiceAccount token for credentials. |
| `S3_STS_AUDIENCE` | Audience the token has to carry (`system-s3-sts`). |
| `S3_STS_CA_CERT` | CA that signs the credentials endpoint. It is the module's own, so nothing in the cluster trusts it by default. |

**There are no credentials in it, and none anywhere else.** The module issues no
long-lived key for a bucket — not for its owner either. Every consumer, including
the component that asked for the bucket, gets credentials the same way: it
presents its ServiceAccount token at the credentials endpoint and receives a key
pair the backend stops honouring an hour later.

`envFrom` this ConfigMap for the addresses, and mount the token:

```yaml
    envFrom:
      - configMapRef:
          name: loki-s3-connection
    volumeMounts:
      - name: s3-token
        mountPath: /var/run/secrets/system-s3
        readOnly: true
      - name: s3-connection
        mountPath: /etc/s3
        readOnly: true
    volumes:
      - name: s3-token
        projected:
          sources:
            - serviceAccountToken:
                path: token
                audience: system-s3-sts     # S3_STS_AUDIENCE
                expirationSeconds: 3600
      - name: s3-connection
        configMap:
          name: loki-s3-connection          # for S3_STS_CA_CERT as a file
```

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
response (an hour by default, `DurationSeconds` between 15 minutes and 12 hours).
There is no session token: the credentials are a real key pair, and S3 requests
signed with them need nothing else.

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

**Reuse the key you are given until it expires.** Every request mints a new key,
and that key stays alive for the rest of its lifetime (an hour by default)
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

`status.conditions` reports the stages in order:

| Condition | Meaning when `False` |
|-----------|----------------------|
| `StorageReady` | The Garage data plane is not serving yet. The message says what it is waiting for (pods rolling out, layout converging). Nothing else can proceed. |
| `BucketReady` | The bucket could not be created in the backend. |
| `ConnectionReady` | The `ConfigMap` with the connection parameters could not be written. |
| `Ready` | Aggregate of the above. |

A `SystemBucket` in a namespace without the `heritage: deckhouse` label is rejected by the webhook. If one exists anyway (created while the webhook was down), the controller refuses to provision it and says so with `StorageReady=False`, reason `NamespaceNotPlatform`.

To inspect the storage itself:

```shell
kubectl -n d8-system-s3 get pods,sts,pvc
kubectl -n d8-system-s3 logs deploy/controller -c controller
```

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
