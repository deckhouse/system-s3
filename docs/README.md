---
title: "Module system-s3"
description: "Deckhouse Kubernetes Platform module that provides the platform's system S3 object storage."
weight: 1
---

{{< alert level="warning" >}}
The module is in the `Experimental` stage. The API, configuration, and custom resources may change without notice; do not use it for production workloads.
{{< /alert >}}

The `system-s3` module provides the **system** S3 object storage: the bucket a Deckhouse component keeps its data in. It runs one Garage cluster on the control-plane nodes and hands out buckets with credentials through a single custom resource.

It is not a storage service for applications. Workloads get their object storage from the `sds-object` module, which offers the user-facing backends; this module exists so platform components have somewhere to put their data without waiting for that one, and it is deliberately the smaller of the two.

## Custom resource

| Resource | Scope | Purpose |
|----------|-------|---------|
| `SystemBucket` (`sb`) | Namespaced | Asks for a bucket in the system storage and receives the S3 credentials `Secret` in the same namespace. |

That is the whole API, and each omission is deliberate:

- **No resource for the storage itself.** The module runs exactly one data plane; its settings live in `ModuleConfig`. There is nothing to reference and no name to know.
- **No separate access resource.** The credentials come with the bucket. Rotate them by changing the `storage.deckhouse.io/rotate` annotation on the `SystemBucket`.
- **No reclaim policy.** Deleting a `SystemBucket` deletes the bucket and every object in it. A platform component owns its bucket outright.

## Who may use it

`SystemBucket` is admitted only in namespaces labelled `heritage: deckhouse` — the label Deckhouse puts on the namespace of every module. The check is on the label rather than on a `d8-*` name, because a name is a convention any tenant could imitate.

No user-facing RBAC is granted for the resource: only cluster and storage administrators (the `d8:manage:permission:module:system-s3:*` roles) can see or edit it.

## The data plane

One Garage cluster, deployed by the controller:

- three replicas by default with a replication factor of 3, independent of the control-plane node count — replicas spread one per master when several exist and co-locate on the single master otherwise;
- storage is a node-sticky local volume per replica (a `hostPath`-backed, node-pinned `PersistentVolume` on the `system-s3-local` StorageClass), so a replica returns to its node — and its data — after a restart;
- `storage.singleReplica` trades all of that for a single replica with replication factor 1.

{{< alert level="warning" >}}
Toggling `storage.singleReplica` on a running installation **destroys everything stored in the system object storage**: Garage cannot change its replication factor in place, so the controller rebuilds the data plane empty. Buckets are recreated and access keys re-issued automatically; the objects are not.
{{< /alert >}}

Garage is used as-is, through its standard admin API. It is AGPL-licensed, which is why it backs the platform's own storage only and is never offered as a service to users.
