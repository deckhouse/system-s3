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

package persistedca

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	tlscertificate "github.com/deckhouse/module-sdk/common-hooks/tls-certificate"
	"github.com/deckhouse/module-sdk/pkg"
	"github.com/deckhouse/module-sdk/pkg/certificate"
	patchablevalues "github.com/deckhouse/module-sdk/pkg/patchable-values"
	"github.com/deckhouse/module-sdk/testing/helpers"
	"github.com/tidwall/gjson"
)

const caPath = "systemS3.internal.customWebhookCA"

func newCA(t *testing.T) *certificate.Authority {
	t.Helper()
	auth, err := certificate.GenerateCA("system-s3-webhooks")
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

// TestTheSchemaDefaultIsNoCA: the values schema defaults the CA to an empty
// object, which exists and must still read as "nothing to use" — or the restore
// would never run and a new CA would be generated as before.
func TestTheSchemaDefaultIsNoCA(t *testing.T) {
	pem := `{"crt":"-----BEGIN CERTIFICATE-----\n","key":"-----BEGIN RSA PRIVATE KEY-----\n"}`
	cases := map[string]bool{
		`{}`:                          false,
		`{"crt":"Zm9v"}`:              false,
		`{"crt":"","key":""}`:         false,
		`{"crt":"Zm9v","key":"Zm9v"}`: false,
		pem:                           true,
	}
	for raw, want := range cases {
		if got := hasCA(gjson.Parse(raw), tlscertificate.GenSelfSignedTLSHookConf{}); got != want {
			t.Errorf("hasCA(%s)=%v, want %v", raw, got, want)
		}
	}
	if hasCA(gjson.Result{}, tlscertificate.GenSelfSignedTLSHookConf{}) {
		t.Error("a missing path reads as a CA")
	}
}

// TestTheSecretRoundTripsTheCA: what the chart renders into the Secret (the
// base64 of each PEM in the values) comes back as the same CA.
func TestTheSecretRoundTripsTheCA(t *testing.T) {
	auth := newCA(t)
	got, err := authorityFrom(secretData(auth))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Cert, auth.Cert) || !bytes.Equal(got.Key, auth.Key) {
		t.Fatal("the CA read back from the Secret is not the one written into it")
	}
}

func TestAnUnreadableSecretIsRefused(t *testing.T) {
	good := base64.StdEncoding.EncodeToString(newCA(t).Cert)
	for name, p := range map[string]persisted{
		"not base64":        {Crt: "!!", Key: good},
		"not a certificate": {Crt: base64.StdEncoding.EncodeToString([]byte("hello")), Key: good},
		"no key":            {Crt: good, Key: ""},
	} {
		if _, err := authorityFrom(p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestTheOverlayShowsTheRestoredCA: module-sdk reads the CA with GetOk on the
// path and unmarshals it; through the overlay it has to see the restored CA, and
// every other path the values underneath.
func TestTheOverlayShowsTheRestoredCA(t *testing.T) {
	under, err := patchablevalues.NewPatchableValues(map[string]any{
		"systemS3": map[string]any{
			"internal": map[string]any{"customWebhookCA": map[string]any{}, "other": "kept"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	auth := newCA(t)
	raw, _ := json.Marshal(map[string]string{"crt": string(auth.Cert), "key": string(auth.Key)})
	var values pkg.PatchableValuesCollector = overlay{PatchableValuesCollector: under, path: caPath, raw: string(raw)}

	v, ok := values.GetOk(caPath)
	if !ok {
		t.Fatal("the CA path is not there")
	}
	if v.Get("crt").String() != string(auth.Cert) {
		t.Error("GetOk on the CA path does not return the restored CA")
	}
	if values.Get(caPath+".crt").String() != string(auth.Cert) {
		t.Error("a path under the CA does not resolve through the overlay")
	}
	if values.Get("systemS3.internal.other").String() != "kept" {
		t.Error("the overlay hides values it has nothing to do with")
	}
}

// secretData is what the chart renders into the Secret from the values: the
// base64 of each PEM.
func secretData(auth *certificate.Authority) persisted {
	return persisted{
		Crt: base64.StdEncoding.EncodeToString(auth.Cert),
		Key: base64.StdEncoding.EncodeToString(auth.Key),
	}
}

func testConf() tlscertificate.GenSelfSignedTLSHookConf {
	return tlscertificate.GenSelfSignedTLSHookConf{
		CommonCAValuesPath:    caPath,
		CommonCACanonicalName: "system-s3-webhooks",
		CN:                    "webhooks",
		TLSSecretName:         "webhooks-https-certs",
		Namespace:             "d8-system-s3",
		SANs:                  tlscertificate.DefaultSANs([]string{"webhooks"}),
		FullValuesPathPrefix:  "systemS3.internal.customWebhookCert",
	}
}

// runHook runs the hook the way Deckhouse does, with module-sdk's own logic
// underneath, and returns the values it leaves behind at path.
func runHook(t *testing.T, ca map[string]any, secret *persisted, path string) gjson.Result {
	t.Helper()
	values, err := run(t, handler(testConf()), ca, secret)
	if err != nil {
		t.Fatal(err)
	}
	return valueAt(values, path)
}

func run(t *testing.T, hook func(context.Context, *pkg.HookInput) error, ca map[string]any, secret *persisted) (pkg.PatchableValuesCollector, error) {
	t.Helper()
	values := helpers.NewValues(map[string]any{
		"global":   map[string]any{"discovery": map[string]any{"clusterDomain": "cluster.local"}},
		"systemS3": map[string]any{"internal": map[string]any{"customWebhookCA": ca}},
	})
	snaps := helpers.NewSnapshots()
	if secret != nil {
		snaps.Add(snapshotKey, helpers.SnapshotFromObject(secret))
	}
	in := helpers.NewInputBuilder(t).WithSnapshots(snaps).WithValues(values).Build()
	return values, hook(context.Background(), in)
}

func valueAt(values pkg.PatchableValuesCollector, path string) gjson.Result {
	pointer := "/" + strings.ReplaceAll(path, ".", "/")
	got := values.Get(path)
	for _, op := range values.GetPatches() {
		switch {
		case op.Path == pointer:
			got = gjson.ParseBytes(op.Value)
		case strings.HasPrefix(pointer, op.Path+"/"):
			got = gjson.GetBytes(op.Value, strings.ReplaceAll(strings.TrimPrefix(pointer, op.Path+"/"), "/", "."))
		}
	}
	return got
}

// TestAFirstInstallKeepsThePEMInTheValues: on a first install module-sdk
// generates the CA and keeps each PEM in the values as it is. The chart has to
// encode it for the Secret's data; the API server refuses the PEM itself there
// with "illegal base64 data at input byte 0".
func TestAFirstInstallKeepsThePEMInTheValues(t *testing.T) {
	crt := runHook(t, map[string]any{}, nil, caPath+".crt").String()
	key := runHook(t, map[string]any{}, nil, caPath+".key").String()
	if !strings.HasPrefix(crt, "-----BEGIN CERTIFICATE-----") || !strings.Contains(key, "PRIVATE KEY-----") {
		t.Fatalf("the generated CA is not kept as PEM: %q, %q", crt, key)
	}
	auth := &certificate.Authority{Cert: []byte(crt), Key: []byte(key)}
	if _, err := authorityFrom(secretData(auth)); err != nil {
		t.Fatalf("the Secret rendered from the values is not read back: %v", err)
	}
}

// TestARestartKeepsTheCAOfTheSecret: Deckhouse starts with empty values and the
// Secret in place; the CA in the values and the one the serving certificate is
// signed with are the Secret's, not a new one.
func TestARestartKeepsTheCAOfTheSecret(t *testing.T) {
	auth := newCA(t)
	stored := secretData(auth)

	if got := runHook(t, map[string]any{}, &stored, caPath+".crt").String(); got != string(auth.Cert) {
		t.Errorf("the values hold a CA other than the Secret's: %q", got)
	}
	if got := runHook(t, map[string]any{}, &stored, "systemS3.internal.customWebhookCert.ca").String(); got != string(auth.Cert) {
		t.Errorf("the serving certificate is signed with a CA other than the Secret's: %q", got)
	}
}

// TestBase64ValuesTakeTheCAFromTheSecret: module-sdk v0.7.0 kept the base64 of
// each PEM in the values, which module-sdk v0.13 cannot parse and replaces with
// a new CA. Values of that shape left from an earlier release are read as no CA,
// and the CA is taken from the Secret.
func TestBase64ValuesTakeTheCAFromTheSecret(t *testing.T) {
	auth := newCA(t)
	stored := secretData(auth)
	old := map[string]any{"crt": stored.Crt, "key": stored.Key}

	if got := runHook(t, old, &stored, caPath+".crt").String(); got != string(auth.Cert) {
		t.Errorf("the values hold a CA other than the Secret's: %q", got)
	}
}

func values(auth *certificate.Authority) map[string]any {
	return map[string]any{"crt": string(auth.Cert), "key": string(auth.Key)}
}

// caValidFor is a CA valid from notBefore for validity, written as module-sdk
// writes one.
func caValidFor(t *testing.T, notBefore time.Time, validity time.Duration) *certificate.Authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "system-s3-webhooks"},
		NotBefore:             notBefore,
		NotAfter:              notBefore.Add(validity),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return &certificate.Authority{
		Cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		Key:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

const tenYears = 24 * time.Hour * 365 * 10

// TestTheSecretWinsOverAnotherCAInTheValues: v0.1.2 left a CA of its own in the
// values while the Secret kept the one consumers trust. The Secret's CA is put
// back, and the serving certificate is signed with it.
func TestTheSecretWinsOverAnotherCAInTheValues(t *testing.T) {
	a, b := newCA(t), newCA(t)
	stored := secretData(a)

	if got := runHook(t, values(b), &stored, caPath+".crt").String(); got != string(a.Cert) {
		t.Errorf("the values hold a CA other than the Secret's: %q", got)
	}
	if got := runHook(t, values(b), &stored, "systemS3.internal.customWebhookCert.ca").String(); got != string(a.Cert) {
		t.Errorf("the serving certificate is signed with a CA other than the Secret's: %q", got)
	}
}

// TestARotationIsNotRolledBack: a CA in the Secret that module-sdk is due to
// renew does not win; the renewed CA in the values stays, on this run and on
// the next one, before the chart has written it into the Secret.
func TestARotationIsNotRolledBack(t *testing.T) {
	for name, due := range map[string]*certificate.Authority{
		"less than half a year left": caValidFor(t, time.Now().Add(-tenYears+90*24*time.Hour), tenYears),
		"another validity":           caValidFor(t, time.Now(), 365*24*time.Hour),
	} {
		stored := secretData(due)

		values, err := run(t, handler(testConf()), map[string]any{}, &stored)
		if err != nil {
			t.Fatal(err)
		}
		crt, key := valueAt(values, caPath+".crt").String(), valueAt(values, caPath+".key").String()
		if crt == string(due.Cert) || !strings.Contains(crt, "BEGIN CERTIFICATE") {
			t.Errorf("%s: the CA due for renewal was not renewed: %q", name, crt)
			continue
		}
		renewed := map[string]any{"crt": crt, "key": key}
		if got := runHook(t, renewed, &stored, caPath+".crt").String(); got != crt {
			t.Errorf("%s: the next run rolled the renewal back: %q", name, got)
		}
	}
}

// TestRotationDueIsModuleSDKsRule: rotationDue says "due" for exactly the CAs
// module-sdk's own hook replaces.
func TestRotationDueIsModuleSDKsRule(t *testing.T) {
	for _, c := range []struct {
		name string
		auth *certificate.Authority
		due  bool
	}{
		{"fresh", newCA(t), false},
		{"a validity of ten years", caValidFor(t, time.Now(), tenYears), false},
		{"seven months left", caValidFor(t, time.Now().Add(-tenYears+210*24*time.Hour), tenYears), false},
		{"five months left", caValidFor(t, time.Now().Add(-tenYears+150*24*time.Hour), tenYears), true},
		{"a validity of one year", caValidFor(t, time.Now(), 365*24*time.Hour), true},
	} {
		due, err := rotationDue(c.auth.Cert, testConf())
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if due != c.due {
			t.Errorf("%s: rotationDue=%v, want %v", c.name, due, c.due)
		}
		values, err := run(t, tlscertificate.GenSelfSignedTLS(testConf()), values(c.auth), nil)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if replaced := valueAt(values, caPath+".crt").String() != string(c.auth.Cert); replaced != due {
			t.Errorf("%s: rotationDue=%v, module-sdk replaced it: %v", c.name, due, replaced)
		}
	}
}

func TestAnUnreadableSecretIsAnErrorWithACAInTheValues(t *testing.T) {
	broken := persisted{Crt: base64.StdEncoding.EncodeToString([]byte("-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----\n")), Key: secretData(newCA(t)).Key}
	if _, err := run(t, handler(testConf()), values(newCA(t)), &broken); err == nil {
		t.Error("a Secret whose certificate does not parse was accepted")
	}
}
