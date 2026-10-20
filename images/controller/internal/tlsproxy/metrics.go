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
	"crypto/subtle"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The traffic of the data plane, as only the terminator sees it.
//
// Garage counts S3 requests by operation and status and nothing by size: its
// only byte counters are block reads and writes on the node's disk, which count
// every replica's copy and the resync alongside what clients sent. Every
// client connection passes through the terminator, so it counts the bytes clients
// sent and received — per listener, which separates S3 from the admin API. These
// are of plaintext HTTP, headers included.
//
// On the S3 route it also counts per bucket: requests by method and status class,
// the body bytes of requests and responses, and how long a request took from its
// headers to the last byte of its response. The bucket labels come from the
// allowlist (buckets.go), never from the request, and the method and status are
// reduced to a handful of values, so a client cannot grow the series count.
// The duration includes the body transfer, so a large object over a slow link is
// a long request; it is a measure of what the client experienced, not of
// Garage's latency alone.
//
// Every byte counter grows as the bytes flow rather than when a connection or a
// request ends: a multi-gigabyte upload is a steady line on a graph, not one step
// at the end.
const (
	// EnvMetricsListen is where the terminator serves its /metrics, over TLS
	// with the data plane's certificate; empty means it does not serve them.
	EnvMetricsListen = "TLS_METRICS_LISTEN"
	// EnvMetricsToken is the bearer token /metrics requires: the token that
	// opens Garage's own /metrics, so that one Secret and one PodMonitor cover
	// both. Without it every scrape is refused.
	EnvMetricsToken = "TLS_METRICS_TOKEN"
	// EnvGarageMetaDir is Garage's metadata_dir as the terminator sees it, on
	// the replica's volume mounted read-only; empty means the size of the
	// metadata is not published.
	EnvGarageMetaDir = "TLS_GARAGE_META_DIR"
)

// Metrics are the terminator's counters, in a registry of their own.
type Metrics struct {
	registry    *prometheus.Registry
	received    *prometheus.CounterVec
	sent        *prometheus.CounterVec
	connections *prometheus.CounterVec

	bucketRequests *prometheus.CounterVec
	bucketReceived *prometheus.CounterVec
	bucketSent     *prometheus.CounterVec
	bucketDuration *prometheus.HistogramVec
}

// bucketLabels are the labels every per-bucket metric carries, the same as the
// controller's system_s3_bucket_* metrics, so that the two join on them.
var bucketLabels = []string{"bucket", "namespace", "name"}

// NewMetrics registers the terminator's counters.
func NewMetrics() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		received: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "system_s3",
			Name:      "proxy_received_bytes_total",
			Help:      "Bytes the TLS terminator received from clients and passed to Garage, by listener port.",
		}, []string{"port"}),
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "system_s3",
			Name:      "proxy_sent_bytes_total",
			Help:      "Bytes the TLS terminator passed from Garage back to clients, by listener port.",
		}, []string{"port"}),
		connections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "system_s3",
			Name:      "proxy_connections_total",
			Help:      "Client connections that completed the TLS handshake, by listener port.",
		}, []string{"port"}),
	}
	m.bucketRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "system_s3",
		Name:      "proxy_bucket_requests_total",
		Help:      "S3 requests through the TLS terminator, by bucket, method and status class.",
	}, append(append([]string{}, bucketLabels...), "method", "code"))
	m.bucketReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "system_s3",
		Name:      "proxy_bucket_received_bytes_total",
		Help:      "S3 request body bytes the TLS terminator passed from clients to Garage, by bucket.",
	}, bucketLabels)
	m.bucketSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "system_s3",
		Name:      "proxy_bucket_sent_bytes_total",
		Help:      "S3 response body bytes the TLS terminator passed from Garage to clients, by bucket.",
	}, bucketLabels)
	m.bucketDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "system_s3",
		Name:      "proxy_bucket_request_duration_seconds",
		Help:      "Time from an S3 request's headers to the end of its response, body transfer included, by bucket.",
		Buckets:   []float64{.005, .025, .1, .25, 1, 5, 30},
	}, bucketLabels)
	m.registry.MustRegister(m.received, m.sent, m.connections,
		m.bucketRequests, m.bucketReceived, m.bucketSent, m.bucketDuration)
	return m
}

// WatchMetadata publishes the size of Garage's metadata in dir, measured on
// every scrape.
//
// The metadata is an LMDB file that grows and does not shrink: pages freed by
// deletions are reused only when no long read holds them, and a sweep that
// read a whole bucket in one transaction once let it grow to most of a volume
// while every other replica's stayed at a fifth of that. Nothing else shows
// it — Garage reports the free space of the volume, which data fills too — and
// the terminator is the one process in the pod that is this module's code and
// can see the volume.
func (m *Metrics) WatchMetadata(dir string) {
	m.registry.MustRegister(&metadataCollector{dir: dir, desc: prometheus.NewDesc(
		prometheus.BuildFQName(metadataSizeOpts.Namespace, "", metadataSizeOpts.Name),
		metadataSizeOpts.Help, nil, nil)})
}

var metadataSizeOpts = prometheus.GaugeOpts{
	Namespace: "system_s3",
	Name:      "replica_metadata_bytes",
	Help:      "Size of Garage's metadata database on this replica (the LMDB files and any metadata snapshots), in bytes.",
}

// metadataCollector measures the metadata directory when it is scraped.
//
// A directory it cannot read publishes nothing rather than zero: zero would be
// a metadata database that is empty, which is a claim, and the replica that
// cannot be measured is better shown as a gap.
type metadataCollector struct {
	dir  string
	desc *prometheus.Desc
}

func (c *metadataCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *metadataCollector) Collect(ch chan<- prometheus.Metric) {
	size, err := metadataSize(c.dir)
	if err != nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(size))
}

// metadataSize sums the regular files of Garage's metadata directory: the ones
// directly in it, those of the LMDB environment in db.lmdb/, and those of each
// snapshot in snapshots/<name>/.
//
// It reads those directories and no deeper on purpose. The metadata sits on
// the same volume as the data blocks, and the data directory of a full replica
// holds millions of files: a walk that strayed into it would turn every scrape
// into minutes of disk reads.
func metadataSize(dir string) (int64, error) {
	total, err := filesIn(dir)
	if err != nil {
		return 0, err
	}
	for _, sub := range []string{"db.lmdb", "snapshots"} {
		n, _ := filesIn(filepath.Join(dir, sub))
		total += n
	}
	snapshots, _ := os.ReadDir(filepath.Join(dir, "snapshots"))
	for _, e := range snapshots {
		if e.IsDir() {
			n, _ := filesIn(filepath.Join(dir, "snapshots", e.Name()))
			total += n
		}
	}
	return total, nil
}

// filesIn is the total size of the regular files directly in dir.
func filesIn(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total, nil
}

// Handler serves the counters to whoever presents token, and to no one when
// token is empty.
func (m *Metrics) Handler(token string) http.Handler {
	inner := promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		inner.ServeHTTP(w, r)
	})
}

// listener returns the counters of one listener port; nil when m is.
func (m *Metrics) listener(port string) *listenerMetrics {
	if m == nil {
		return nil
	}
	return &listenerMetrics{
		received:    m.received.WithLabelValues(port),
		sent:        m.sent.WithLabelValues(port),
		connections: m.connections.WithLabelValues(port),
	}
}

type listenerMetrics struct {
	received, sent, connections prometheus.Counter
}

// bucket returns the per-bucket metrics of one bucket; nil ones when m is nil.
func (m *Metrics) bucket(bucket, namespace, name string) bucketMetrics {
	if m == nil {
		return bucketMetrics{}
	}
	return bucketMetrics{
		received: m.bucketReceived.WithLabelValues(bucket, namespace, name),
		sent:     m.bucketSent.WithLabelValues(bucket, namespace, name),
		duration: m.bucketDuration.WithLabelValues(bucket, namespace, name),
	}
}

type bucketMetrics struct {
	received, sent prometheus.Counter
	duration       prometheus.Observer
}

// counting adds every byte written through it to c.
type counting struct {
	io.Writer
	c prometheus.Counter
}

func (w counting) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.c.Add(float64(n))
	return n, err
}
