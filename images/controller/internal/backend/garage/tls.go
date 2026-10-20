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
	"errors"
	"fmt"
	"net/http"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/tlsclient"
	"github.com/deckhouse/system-s3/images/controller/internal/tlsproxy"
)

// The data plane's TLS.
//
// Garage serves no TLS: its S3 and admin listeners take an address and nothing
// else. So Garage listens on the loopback only, on ports of its own, and the
// terminator — this module's controller binary, run as a sidecar — owns the
// ports every client has always used, now with TLS. The Services and the
// clients keep their port numbers; what changes is that nothing but the
// terminator can reach Garage's plaintext listeners at all, because they are
// not bound to an address anything outside the pod can reach.
//
// RPC between Garage nodes (3901) is not behind the terminator and does not
// need to be: Garage encrypts it itself, with a secret-handshake authenticated
// by the cluster's rpc_secret.
const (
	// garageS3Loopback and garageAdminLoopback are where Garage itself listens.
	garageS3Loopback    = 3910
	garageAdminLoopback = 3913

	// tlsVolume is the data plane's certificate, and tlsMountPath where the
	// terminator (and the migration Job, for the CA alone) reads it.
	tlsVolume    = "garage-tls"
	tlsMountPath = "/etc/garage-tls"

	terminatorContainer = "tls-terminator"

	// bucketsVolume is the bucket-owner record (bucket_owners.go), and
	// bucketsMountPath where the terminator reads it as its allowlist of the
	// buckets its metrics may name.
	bucketsVolume    = "bucket-owners"
	bucketsMountPath = "/etc/garage-buckets"
)

// garageRootDomain is Garage's s3_api.root_domain: a Host under it names the
// bucket. The terminator is told it too, to read the bucket the way Garage does.
const garageRootDomain = ".s3.garage"

// garageServerName is the name the data plane's certificate is verified for.
//
// It is the endpoint Service's name, which the certificate carries. Clients that
// dial a plane Service match its wildcard name anyway; the one that dials pod
// IPs — the admin calls made to one particular node — cannot match anything by
// address, and verifies this name instead.
func garageServerName(store *backend.Store, namespace string) string {
	return fmt.Sprintf("%s.%s.svc", s3SvcName(store), namespace)
}

// admin returns an admin API client that verifies the node against the module's
// CA. baseURL is https.
//
// A CA that cannot be read does not produce a client that skips verification:
// it produces one whose every call fails with the reason, so the phase reports
// "cannot read the Garage CA" rather than the controller handing its admin token
// to whatever answered.
func (d *Driver) admin(store *backend.Store, baseURL, token string) *adminClient {
	c := newAdminClient(baseURL, token)
	t, err := d.garageCA.Transport(garageServerName(store, d.namespace))
	if err != nil {
		c.http.Transport = failingTransport{err: err}
		return c
	}
	c.http.Transport = t
	return c
}

// failingTransport is what an admin client uses when there is no CA to verify
// Garage with.
type failingTransport struct{ err error }

func (f failingTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, f.err }

// WithTLS tells the driver which Secret holds the data plane's certificate and
// where the controller's own copy of its CA is mounted.
func (d *Driver) WithTLS(secretName, caFile string) *Driver {
	d.tlsSecret = secretName
	d.garageCA = tlsclient.NewCA(caFile)
	return d
}

// tlsConfigured reports whether the driver can bring up a data plane at all.
//
// Without a certificate there is no data plane rather than a plaintext one:
// the listeners Garage would expose are exactly the ones the terminator exists
// to keep off the network.
func (d *Driver) tlsConfigured() error {
	if d.tlsSecret == "" || d.garageCA == nil || d.garageCA.Path() == "" {
		return errors.New("the data plane's TLS certificate is not configured " +
			"(GARAGE_TLS_SECRET and GARAGE_TLS_CA_FILE are empty); the chart renders both")
	}
	return nil
}

// terminatorSidecar is the TLS terminator in the Garage pod.
func terminatorSidecar(image string) corev1.Container {
	return corev1.Container{
		Name:  terminatorContainer,
		Image: image,
		Args:  []string{tlsproxy.Command},
		Env: []corev1.EnvVar{
			{Name: tlsproxy.EnvRoutes, Value: fmt.Sprintf("[::]:%d=127.0.0.1:%d,[::]:%d=127.0.0.1:%d",
				s3Port, garageS3Loopback, adminPort, garageAdminLoopback)},
			{Name: tlsproxy.EnvCertFile, Value: tlsMountPath + "/tls.crt"},
			{Name: tlsproxy.EnvKeyFile, Value: tlsMountPath + "/tls.key"},
			// S3 is served as HTTP, to count it per bucket; the admin API stays a
			// TCP copy.
			{Name: tlsproxy.EnvS3Listen, Value: fmt.Sprintf("[::]:%d", s3Port)},
			{Name: tlsproxy.EnvS3RootDomain, Value: garageRootDomain},
			{Name: tlsproxy.EnvBucketsDir, Value: bucketsMountPath},
			{Name: tlsproxy.EnvMetricsListen, Value: fmt.Sprintf("[::]:%d", proxyMetricsPort)},
			// The token that opens Garage's /metrics opens these too: the PodMonitor
			// scrapes both with the one Secret it already has.
			{Name: tlsproxy.EnvMetricsToken, ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: metricsTokenSecret},
					Key:                  metricsTokenKey,
				},
			}},
		},
		// The ports every client dials. They are declared here, on the container
		// that listens on them, and the Services reach them by number.
		Ports: []corev1.ContainerPort{
			{Name: "s3", ContainerPort: s3Port},
			{Name: "admin", ContainerPort: adminPort},
			{Name: proxyMetricsPortName, ContainerPort: proxyMetricsPort},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: tlsVolume, MountPath: tlsMountPath, ReadOnly: true},
			// The whole volume and not a subPath: a subPath mount is never updated,
			// and the list changes with every SystemBucket created or deleted.
			{Name: bucketsVolume, MountPath: bucketsMountPath, ReadOnly: true},
		},
		// Proxying costs little memory, bodies being streamed; the CPU is the TLS
		// itself, and it scales with the traffic rather than with anything this
		// request could predict, so there is no limit to throttle it by.
		Resources:       corev1.ResourceRequirements{Requests: terminatorRequests()},
		SecurityContext: hardenedRoot(),
	}
}

// withTLSTerminator puts the terminator into the data plane's pod template,
// together with the certificate it serves and the bucket-owner record
// bucketsConfigMap it names buckets by.
//
// Neither is part of anything the pods roll on: a renewal replaces the files in
// the mounted Secret and the terminator picks them up on the next handshake, and
// a new bucket reaches it the same way, so neither renewing a certificate nor
// creating a SystemBucket restarts a replica.
func withTLSTerminator(sts *appsv1.StatefulSet, image, secretName, bucketsConfigMap string) {
	spec := &sts.Spec.Template.Spec
	spec.Containers = append(spec.Containers, terminatorSidecar(image))
	spec.Volumes = append(spec.Volumes, tlsSecretVolume(secretName), bucketsConfigMapVolume(bucketsConfigMap))
}

// bucketsConfigMapVolume is the bucket-owner record as a pod volume.
//
// Optional, because the record is created by the first claim and a data plane
// comes up before any SystemBucket has one; the kubelet fills the volume in once
// the ConfigMap exists.
func bucketsConfigMapVolume(name string) corev1.Volume {
	return corev1.Volume{
		Name: bucketsVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: name},
				Optional:             ptr.To(true),
			},
		},
	}
}

// tlsSecretVolume is the certificate Secret as a pod volume.
func tlsSecretVolume(secretName string) corev1.Volume {
	return corev1.Volume{
		Name: tlsVolume,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: secretName},
		},
	}
}

// caOnlyVolume is the CA of the certificate Secret, and nothing else, for a pod
// that has to verify Garage but has no business holding its private key.
func caOnlyVolume(secretName string) corev1.Volume {
	return corev1.Volume{
		Name: tlsVolume,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: secretName,
				Items:      []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
			},
		},
	}
}
