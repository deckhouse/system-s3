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
spec: {}
```

The controller creates the bucket in the system storage, issues an access key for it and writes a `Secret` next to the resource:

```console
$ kubectl -n d8-monitoring get sb
NAME   BUCKET                 PHASE   SECRET               READY   AGE
loki   d8-monitoring-loki     Ready   loki-s3-credentials  True    30s
```

The `Secret` holds the standard S3 connection variables, so a workload can consume it with `envFrom`:

```yaml
    envFrom:
      - secretRef:
          name: loki-s3-credentials
```

| Key | Value |
|-----|-------|
| `S3_ENDPOINT` | In-cluster endpoint of the system storage. |
| `S3_REGION` | Region the endpoint advertises (`garage`). |
| `S3_BUCKET` | Effective bucket name. |
| `AWS_ACCESS_KEY_ID` | Access key id issued for this bucket. |
| `AWS_SECRET_ACCESS_KEY` | Secret key. Kept only here — the backend cannot recover it. |

## Bucket names

`spec.bucketName` is optional. When it is omitted the bucket is named `<namespace>-<name>`, because the backend's bucket names are flat while `SystemBucket` is namespaced: two components asking for `backups` must not end up sharing one bucket.

Set it explicitly only when something outside the cluster needs a specific name. Two `SystemBucket` resources may not map to the same bucket name: the admission webhook rejects the second one, and if it is bypassed — two simultaneous creations, or a cluster where the webhook is not installed — the controller refuses it as well, reporting `BucketReady=False` with the owner it belongs to. Give that resource a different `spec.bucketName`.

The refusal is not caution for its own sake: a `SystemBucket` owns its data outright and there is no reclaim policy, so two owners of one bucket means both write to the same storage and deleting either one destroys the other's objects.

## Rotating credentials

Change the value of the `storage.deckhouse.io/rotate` annotation:

```shell
kubectl -n d8-monitoring annotate systembucket loki \
  storage.deckhouse.io/rotate="$(date +%s)" --overwrite
```

The controller issues a fresh key pair, updates the `Secret`, and revokes the previous key **on a later pass** — `status.supersededAccessKeyID` holds it in the meantime. The order is deliberate: the backend hands back a key's secret half once and never again, so revoking before the new secret is stored would leave the consumer holding a key that no longer works while its replacement became unusable to everyone. For a few seconds after a rotation both keys work. `status.observedRotation` records the value it processed and `status.lastRotationTime` when. Restart the consuming workload if it does not re-read the `Secret` by itself.

The same happens when the credentials `Secret` is deleted: it is owned by the `SystemBucket`, so its removal re-drives the resource and a new key pair is issued.

## Deleting a bucket

```shell
kubectl -n d8-monitoring delete systembucket loki
```

{{< alert level="danger" >}}
This deletes the bucket and every object in it. There is no reclaim policy and no recycle bin — make sure the data is no longer needed, or copied elsewhere, first.
{{< /alert >}}

The resource keeps a finalizer until the bucket and its access key are gone from the backend, so a failure to reach the storage delays the deletion instead of orphaning data in it.

## Status and troubleshooting

`status.conditions` reports the stages in order:

| Condition | Meaning when `False` |
|-----------|----------------------|
| `StorageReady` | The Garage data plane is not serving yet. The message says what it is waiting for (pods rolling out, layout converging). Nothing else can proceed. |
| `BucketReady` | The bucket could not be created in the backend. |
| `CredentialsReady` | The access key could not be issued, or the `Secret` could not be written. |
| `Ready` | Aggregate of the above. |

A `SystemBucket` in a namespace without the `heritage: deckhouse` label is rejected by the webhook. If one exists anyway (created while the webhook was down), the controller refuses to provision it and says so with `StorageReady=False`, reason `NamespaceNotPlatform`.

To inspect the storage itself:

```shell
kubectl -n d8-system-s3 get pods,sts,pvc
kubectl -n d8-system-s3 logs deploy/controller -c controller
```
