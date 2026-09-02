# system-s3

Deckhouse Kubernetes Platform module that provides the platform's **system** S3
object storage: a Garage data plane the module owns end to end, and a single
`SystemBucket` custom resource with which a Deckhouse component asks for a bucket
and receives S3 credentials in its own namespace.

> The module is in the `Experimental` stage. The API, configuration, and custom
> resources may change without notice; do not use it for production workloads.

The storage is for platform components only:

- `SystemBucket` is accepted solely in namespaces labelled `heritage: deckhouse`,
  and no user-facing RBAC is granted for it;
- credentials come with the bucket — there is no separate access resource;
- deleting a `SystemBucket` deletes the bucket in the backend.

User-facing object storage (SeaweedFS, Ceph RGW) lives in the separate
[sds-object](https://github.com/deckhouse/sds-object) module. Garage is
AGPL-licensed and is therefore never offered as a user-facing service — it backs
this module's system storage only.
