---
title: "Module system-s3"
description: "Deckhouse Kubernetes Platform module that provides the platform's system S3 object storage."
weight: 1
---

{{< alert level="warning" >}}
The module is in the `Experimental` stage. The API, configuration, and custom resources may change without notice; do not use it for production workloads.
{{< /alert >}}

The `system-s3` module provides the **system** S3 object storage for Deckhouse components. It runs one Garage cluster on nodes selected by the `system-s3` role, then `system`, then other eligible nodes. `SystemBucket` provisions the bucket and connection parameters; consumers obtain temporary credentials separately through STS using a ServiceAccount token and RBAC.

It is not a storage service for applications. Workloads get their object storage from the `sds-object` module, which offers the user-facing backends; this module exists so platform components have somewhere to put their data without waiting for that one, and it is deliberately the smaller of the two.

## Custom resource

| Resource | Scope | Purpose |
|----------|-------|---------|
| `SystemBucket` (`sb`) | Namespaced | Asks for a bucket in the system storage and receives a `ConfigMap` with the S3 connection parameters in the same namespace. |

That is the whole API, and each omission is deliberate:

- **No resource for the storage itself.** The module runs exactly one data plane; its settings live in `ModuleConfig`. There is nothing to reference and no name to know.
- **No separate access resource.** RBAC on `systembuckets/read` and `systembuckets/write` controls access. STS issues temporary keys; this contract has no long-lived consumer keys or manual rotation annotation.
- **No reclaim policy.** Deleting a `SystemBucket` deletes the bucket and every object in it. A platform component owns its bucket outright.

## Who may use it

`SystemBucket` is admitted only in namespaces labelled `heritage: deckhouse` — the label Deckhouse puts on the namespace of every module. The check is on the label rather than on a `d8-*` name, because a name is a convention any tenant could imitate.

No user-facing RBAC is granted for the resource: only cluster and storage administrators (the `d8:system-capability:system-s3:*` capabilities on DKP 1.78 and later, the `d8:manage:permission:module:system-s3:*` roles below 1.78) can see or edit it.

## The data plane

One Garage cluster, deployed by the controller:

- three replicas with a replication factor of 3 in the platform's high-availability mode. Required pod anti-affinity keeps them on distinct eligible hosts; fewer than three hosts leave replicas Pending rather than co-locating them;
- how big it is is not a setting: every replica's volume is sized from the sum of the `SystemBucket` sizes (plus 10% headroom and a 2 GiB metadata reserve, at least 10 GiB, rounded up to a multiple of 5 GiB) and grows with it, as long as the StorageClass allows volume expansion;
- storage is one `PersistentVolumeClaim` per replica, ordered from `storageClass` (falling back to `global.modules.storageClass` and then the cluster default). A StorageClass is required — without one no data plane is created, and every `SystemBucket` says so;
- outside that mode, a single replica with replication factor 1 — a third of the footprint, no redundancy. That replica never relocates: on a node-pinned class relocating it would mean losing the whole store (there is no second copy), so when its node goes away it stays Pending with its data intact on that node's disk — the storage is unavailable until the node comes back. On a class whose volumes follow the pod the outage is only the move. The mode follows `highAvailability`, then `global.highAvailability`, and with neither set, whether the control plane itself is highly available. In that last case a shrink is not performed: a cluster that loses masters reads as not highly available, and the store keeps its three replicas until `highAvailability: false` is written out loud;
- what a replica's move costs depends on the class. Where volumes are pinned to nodes (a local class), moving a replica leaves its copy behind and Garage rebuilds it from the others — the next replica moves only once that rebuild is complete, which takes at least ten minutes per move; where volumes follow the pod (a network class), the volume is simply attached elsewhere. The module reads which it is from the volume itself;
- placement follows the platform's rule for system components: the data plane
  runs on nodes with the `node-role.deckhouse.io/system-s3` role if the cluster
  has any, on system nodes (`node-role.deckhouse.io/system`) if it has those
  instead, and on the masters otherwise; it tolerates the `dedicated.deckhouse.io`
  taint with the value `system-s3` or `system`, and on the masters their
  `node-role.kubernetes.io/control-plane` and `node-role.kubernetes.io/master`
  taints. To give the store nodes of its own, give a NodeGroup the `system-s3`
  role — with disks sized for it, since that is where its volumes are ordered on
  a local class. With neither role, the highly available mode needs three
  masters: on a cluster with one master the second and third replicas stay
  Pending until the role is given or `highAvailability: false` is set.
  Required anti-affinity
  separates HA replicas by `kubernetes.io/hostname`. If a `system-s3` role
  exists on too few nodes, placement does not fall back to other roles or
  recycle PVCs to consolidate the replicas. Add eligible nodes or explicitly
  change `highAvailability`. During a migration two Garage clusters run on the
  same nodes side by side; each spreads itself and neither pushes the other off
  a node. The controller itself stays on the control-plane nodes and follows
  control-plane HA independently of the Garage replica mode;
- what this means for the StorageClass: it has to be able to give volumes on those nodes, and with `volumeBindingMode: WaitForFirstConsumer` — with `Immediate` the volume is allocated before a node is chosen, and the replica is bound to wherever it landed.

{{< alert level="warning" >}}
Changing the high-availability mode of an installation that already holds data is done **on the same Garage cluster and the same volumes**. Garage cannot change its replication factor while it runs, so the module stops the data plane, removes the saved cluster layout and starts it again with the other replica count; nothing is copied. Going from one replica to three, the store is unavailable from the stop until the three replicas are up — with duration depending on volumes and startup time — and the new replicas are filled in the background while it serves. Going from three to one, writes are refused (403) until every replica holds everything, while reads keep working; if that takes longer than 10 minutes, writes are given back and the change is tried again later. The store is then unavailable while it restarts on one replica, and the volumes of the two replicas that are gone are released. Temporary keys asked for with write access while writes are refused are refused as well.

Changing the StorageClass — alone or together with the mode — is a **migration between two Garage clusters**. Bound volumes cannot be re-ordered from another class, so the module builds a second cluster next to the first, copies the objects into it and moves the endpoint onto it. Depending on how much there is to copy, that takes minutes or hours.

During cutover writes are refused with a 403. The final pass copies the delta and removes destination objects absent from the frozen source, then checks aggregate object and byte counts, not every object's SHA256. An in-flight multipart upload may need to be retried; the controller sweeps incomplete parts older than a day. Previously issued temporary keys are not imported: consumers must obtain new credentials even before the old `Expiration`. Expiry-based SDK refresh does not guarantee recovery from this early invalidation; test the application's handling of that 403. The endpoint address is stable, but uninterrupted access is not guaranteed. Progress is reported in `Module/system-s3.MigrationReady`; HA transitions use `ReplicationReady`. After the switch the old cluster stays read-only only until every replica of the new one holds every block, and is then removed; there is no rollback to it once it is gone.

Cutover also restarts **every source Garage Pod on its existing PVC**, without purging the layout, so that every request admitted before the freeze has finished. In the highly available mode the Pods are restarted one at a time — the next only after the previous one is back, ready and the cluster has synchronized — so two replicas keep a quorum and the store keeps serving. A read can still stall for up to about a minute while the replica Garage asks first for that object's blocks is away: Garage waits for a peer that has disappeared until it misses four pings, 15 seconds apart. A single-replica store has to be stopped for it, and reads are unavailable during that restart. The final copier starts only after every old Pod has been replaced and the cluster has synchronized; termination is never force-bypassed on timeout. Pending write-key grants are tracked in the migration ConfigMap across controller replicas and restarts. An ambiguous grant RPC retains its barrier until Garage confirms the key has expired; a missing record alone is not proof of expiration. Do not manually remove these tickets or force-delete source Pods to unblock migration. The controller normally abandons a cutover after 30 minutes, stopping copier Pods before allowing writes again; recovery still depends on the source volumes and Pods becoming available.

Objects are skipped only when size and a non-empty ETag agree. Different multipart ETags require copying again, regardless of timestamps, so large multipart datasets can exceed the cutover budget. A confirmed destination quota failure in bulk copy enters bounded frozen synchronization, which prunes stale destination keys before copying replacements. Authentication failures do not trigger this transition and destination quotas are not removed.

Both clusters exist at once for the length of the migration, so the storage it needs is up to twice the usual — more when moving from one replica to three. Putting the setting back before the switch happens calls the migration off and removes the half-built cluster.
{{< /alert >}}

## One way to reach a bucket

There are no long-lived credentials, and no `Secret` anywhere holding a key for a
bucket. A consumer presents its ServiceAccount token at the credentials endpoint
and receives a key pair the backend stops honouring at most thirty minutes later. The endpoint
speaks AWS STS `AssumeRoleWithWebIdentity`, so any S3 client already knows how to
use it; see [USAGE](usage.html).

Who may ask, and for which bucket, is Kubernetes RBAC: a `Role` granting `get` on
`systembuckets/read` or `systembuckets/write` for the named bucket. That includes
the component that created the resource — owning a `SystemBucket` is permission to
edit the resource, not to read the objects in its bucket.

This is what makes access revocable. A stored key is a credential nobody can
account for, and taking it back means rotating it for everyone who holds a copy. A
grant is a `RoleBinding`: deleting it stops the next request, and what was already
issued expires on the backend's clock — nothing has to be rotated, because nothing
was durable in the first place.

The cost is that a component has to be able to do the exchange. Something that can
only read a key from a file or an environment variable cannot use this storage.

Garage is used as-is, through its standard admin API. It is AGPL-licensed, which is why it backs the platform's own storage only and is never offered as a service to users.

## Removing the module

Disabling the module asks for confirmation, and it deletes the system object
storage. The module namespace goes, and with it the Garage workload, its
configuration, its Secrets and the claims of its volumes. What happens to the
volumes behind the claims is the StorageClass's reclaim policy: `Delete` removes
them with the objects in them, `Retain` keeps them. A re-enabled module does not
reattach retained volumes — it starts with an empty store — so data that has to
survive is copied out with an S3 client before the module is disabled.

`SystemBucket` resources are not deleted. The module releases its finalizer on
them so that they can be deleted later without it, and they stay with their
connection ConfigMaps. When the module is enabled again, their buckets are created
again, empty.

Before disabling the module, list the buckets with `kubectl get sb -A`: every
component listed keeps data in the system object storage and loses access to it
the moment the module goes.
