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
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/deckhouse/module-sdk/pkg"
	"github.com/deckhouse/module-sdk/pkg/certificate"
	patchablevalues "github.com/deckhouse/module-sdk/pkg/patchable-values"
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
	cases := map[string]bool{
		`{}`:                          false,
		`{"crt":"Zm9v"}`:              false,
		`{"crt":"","key":""}`:         false,
		`{"crt":"Zm9v","key":"Zm9v"}`: true,
	}
	for raw, want := range cases {
		if got := hasCA(gjson.Parse(raw)); got != want {
			t.Errorf("hasCA(%s)=%v, want %v", raw, got, want)
		}
	}
	if hasCA(gjson.Result{}) {
		t.Error("a missing path reads as a CA")
	}
}

// TestTheSecretRoundTripsTheCA: what the chart renders into the Secret (the
// values' base64 of each PEM) comes back as the same CA.
func TestTheSecretRoundTripsTheCA(t *testing.T) {
	auth := newCA(t)
	values, err := json.Marshal(auth) // what the hook stores and the chart renders
	if err != nil {
		t.Fatal(err)
	}
	var secretData persisted
	if err := json.Unmarshal(values, &secretData); err != nil {
		t.Fatal(err)
	}

	got, err := authorityFrom(secretData)
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
	raw, _ := json.Marshal(auth)
	var values pkg.PatchableValuesCollector = overlay{PatchableValuesCollector: under, path: caPath, raw: string(raw)}

	v, ok := values.GetOk(caPath)
	if !ok {
		t.Fatal("the CA path is not there")
	}
	got := new(certificate.Authority)
	if err := json.Unmarshal([]byte(v.String()), got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Cert, auth.Cert) {
		t.Error("GetOk on the CA path does not return the restored CA")
	}
	if values.Get(caPath+".crt").String() == "" {
		t.Error("a path under the CA does not resolve through the overlay")
	}
	if values.Get("systemS3.internal.other").String() != "kept" {
		t.Error("the overlay hides values it has nothing to do with")
	}
}
