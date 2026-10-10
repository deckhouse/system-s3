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

// Package sts hands a Deckhouse component temporary S3 credentials for a system
// bucket in exchange for its own ServiceAccount token, with Kubernetes RBAC
// deciding whether it may have them.
//
// Why it exists: a SystemBucket's long-lived key is written into a Secret next to
// the resource, which makes the bucket usable by whatever can read that Secret
// and by nothing else. That is the right shape for the component that asked for
// the bucket, and the wrong one for everything else — a second component that
// needs read access to the same bucket has no way to be given it except a copy of
// the key, and a key that has been copied is a key nobody can account for.
//
// So access is granted the way everything else in the cluster is granted: with a
// Role and a RoleBinding. The subject presents the token it already has, this
// endpoint asks the API server who that is (TokenReview) and whether they are
// allowed (SubjectAccessReview), and mints a key that Garage expires on its own.
// Revoking access is deleting a RoleBinding; nothing has to be rotated afterwards
// because nothing long-lived was handed out.
//
// The wire protocol is AWS STS AssumeRoleWithWebIdentity, because every S3 client
// worth using already speaks it: aws-sdk-go's stscreds.WebIdentityRoleProvider and
// minio-go's credentials.NewSTSWebIdentity need an endpoint and a token file, and
// that is the whole integration on the consumer's side.
package sts

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/backend/garage"
	"github.com/deckhouse/system-s3/images/controller/internal/monitoring"
)

// TokenAudience is the audience a caller's ServiceAccount token has to carry.
//
// A projected token bound to this audience is useless anywhere else — it will not
// authenticate to the API server itself — so a component that leaks one leaks the
// ability to ask for credentials it was entitled to anyway, and nothing more. A
// token with the default audience is refused for the same reason in reverse: it
// is the API server's token, and this endpoint has no business accepting it.
const TokenAudience = "system-s3-sts"

// Temporary credentials never live longer than thirty minutes. Requests from
// SDKs that default to an hour are clamped; clients must use the returned expiry.
const (
	DefaultDuration = 30 * time.Minute
	MinDuration     = 15 * time.Minute
	MaxDuration     = 30 * time.Minute
)

// Subresources the endpoint authorises against. They are virtual — no API server
// serves them — and that is the point: "may read the objects in this bucket" is a
// different permission from "may edit the SystemBucket resource", and giving them
// the same name would make a component that is allowed to read a bucket also
// allowed to delete it (which deletes the data).
const (
	subresourceRead  = "read"
	subresourceWrite = "write"
)

// Issuer is the piece of the driver this endpoint needs.
type Issuer interface {
	IssueTemporaryAccess(
		ctx context.Context, store *backend.Store, bucketName, subject string, write bool, expiresAt time.Time,
	) (garage.TemporaryCredentials, error)
}

// Authorizer is the API server, as this endpoint uses it: who is this, and may
// they do that. Narrowed to an interface so the decision logic can be tested
// without a cluster.
type Authorizer interface {
	Authenticate(ctx context.Context, token string) (user authnv1.UserInfo, ok bool, err error)
	Authorize(ctx context.Context, user authnv1.UserInfo, namespace, name, subresource string) (bool, error)
}

// Server serves the credentials endpoint.
type Server struct {
	Reader     client.Reader
	Issuer     Issuer
	Authorizer Authorizer
	Store      *backend.Store
	Log        logger.Logger

	// Now is time.Now unless a test replaced it.
	Now func() time.Time
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Handler is the endpoint's routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("Ok."))
	})
	mux.HandleFunc("/", s.serve)
	return mux
}

// serve implements AssumeRoleWithWebIdentity.
//
// Which bucket is asked for comes from the path — /d8-monitoring/loki — rather
// than from an ARN. An ARN is what AWS uses to name a role in an account; there is
// no account here, and a namespace and a name are what a Kubernetes consumer
// already knows. RoleArn is still accepted, in the one spelling this module
// documents, for clients that insist on sending one.
func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "only POST is accepted")
		return
	}
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "unparseable request body")
		return
	}

	if action := r.Form.Get("Action"); action != "" && action != "AssumeRoleWithWebIdentity" {
		writeError(w, http.StatusBadRequest, "InvalidAction",
			fmt.Sprintf("this endpoint implements AssumeRoleWithWebIdentity, not %q", action))
		return
	}

	namespace, name, err := bucketFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}

	token := webIdentityToken(r)
	if token == "" {
		writeError(w, http.StatusBadRequest, "InvalidRequest",
			"no WebIdentityToken: send the ServiceAccount token as WebIdentityToken or in the Authorization header")
		return
	}

	duration, err := requestedDuration(r.Form.Get("DurationSeconds"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationError", err.Error())
		return
	}

	ctx := r.Context()

	user, ok, err := s.Authorizer.Authenticate(ctx, token)
	if err != nil {
		s.Log.Error("unable to review the presented token", logger.Err(err))
		writeError(w, http.StatusInternalServerError, "InternalFailure", "unable to review the presented token")
		return
	}
	if !ok {
		// Deliberately not saying why. The caller learns whether their token is
		// good; they do not learn anything about the bucket they asked for.
		writeError(w, http.StatusForbidden, "InvalidIdentityToken",
			"the presented token was not accepted (it must be a ServiceAccount token with audience "+TokenAudience+")")
		return
	}

	write, err := s.Authorizer.Authorize(ctx, user, namespace, name, subresourceWrite)
	if err != nil {
		s.Log.Error("unable to check the caller's permissions", logger.Err(err))
		writeError(w, http.StatusInternalServerError, "InternalFailure", "unable to check permissions")
		return
	}
	read := write
	if !write {
		read, err = s.Authorizer.Authorize(ctx, user, namespace, name, subresourceRead)
		if err != nil {
			s.Log.Error("unable to check the caller's permissions", logger.Err(err))
			writeError(w, http.StatusInternalServerError, "InternalFailure", "unable to check permissions")
			return
		}
	}
	if !read {
		writeError(w, http.StatusForbidden, "AccessDenied", fmt.Sprintf(
			"%s is not allowed to use the system bucket %s/%s: grant get on systembuckets/%s (or /%s) for it",
			user.Username, namespace, name, subresourceRead, subresourceWrite))
		return
	}

	bucketName, err := s.backendBucket(ctx, namespace, name)
	if err != nil {
		// The caller is allowed to know this: they were authorised for the resource,
		// so its state is not a secret from them.
		writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}

	expiresAt := s.now().Add(duration)
	creds, err := s.Issuer.IssueTemporaryAccess(ctx, s.Store, bucketName, user.Username, write, expiresAt)
	if err != nil {
		s.Log.Error("unable to issue temporary credentials", logger.Err(err),
			"systemBucket", namespace+"/"+name, "subject", user.Username)
		writeError(w, http.StatusInternalServerError, "InternalFailure", "unable to issue credentials")
		return
	}

	s.Log.Info("issued temporary credentials",
		"systemBucket", namespace+"/"+name, "bucket", bucketName, "subject", user.Username,
		"write", write, "expiresAt", expiresAt.UTC().Format(time.RFC3339), "accessKeyID", creds.AccessKeyID)

	writeCredentials(w, user, creds)
}

// backendBucket resolves the SystemBucket to the bucket name in the backend, and
// refuses one that is not usable yet.
func (s *Server) backendBucket(ctx context.Context, namespace, name string) (string, error) {
	bucket := &v1alpha1.SystemBucket{}
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, bucket); err != nil {
		return "", fmt.Errorf("there is no SystemBucket %s/%s", namespace, name)
	}
	if bucket.Status == nil || bucket.Status.BucketName == "" {
		return "", fmt.Errorf("the SystemBucket %s/%s has no bucket in the backend yet", namespace, name)
	}
	return bucket.Status.BucketName, nil
}

// bucketFromRequest takes the bucket from the path, falling back to RoleArn.
func bucketFromRequest(r *http.Request) (namespace, name string, err error) {
	if ns, n, ok := splitBucketPath(r.URL.Path); ok {
		return ns, n, nil
	}
	if arn := r.Form.Get("RoleArn"); arn != "" {
		if ns, n, ok := splitBucketPath(strings.TrimPrefix(arn, RoleARNPrefix)); ok {
			return ns, n, nil
		}
		return "", "", fmt.Errorf("RoleArn %q is not %s<namespace>/<name>", arn, RoleARNPrefix)
	}
	return "", "", errors.New("no bucket in the request: POST to /<namespace>/<name> or send RoleArn=" +
		RoleARNPrefix + "<namespace>/<name>")
}

// RoleARNPrefix is the ARN spelling this module accepts, for clients that cannot
// put the bucket in the path. It is part of the public API because the
// connection ConfigMap hands it to the SDKs as AWS_ROLE_ARN.
const RoleARNPrefix = v1alpha1.RoleARNPrefix

func splitBucketPath(p string) (namespace, name string, ok bool) {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// webIdentityToken reads the token from the form or from the Authorization
// header. The header is not part of the AWS protocol; it is there because a
// human reaching for curl reaches for it.
func webIdentityToken(r *http.Request) string {
	if t := r.Form.Get("WebIdentityToken"); t != "" {
		return t
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	}
	return ""
}

// requestedDuration clamps DurationSeconds into what this endpoint will issue.
//
// Clamped rather than refused: the AWS SDKs send a duration of their own choosing
// (an hour by default, and some send twelve), and failing a request over a number
// the caller did not pick would make the integration "works with these three
// clients" instead of "works".
func requestedDuration(raw string) (time.Duration, error) {
	if raw == "" {
		return DefaultDuration, nil
	}
	secs, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("DurationSeconds %q is not a number", raw)
	}
	// Clamp seconds before converting: a very large valid integer must not
	// overflow time.Duration and turn a maximum request into a minimum one.
	if secs < int(MinDuration/time.Second) {
		return MinDuration, nil
	}
	if secs > int(MaxDuration/time.Second) {
		return MaxDuration, nil
	}
	return time.Duration(secs) * time.Second, nil
}

// --- the wire format --------------------------------------------------------

type assumeRoleResponse struct {
	XMLName xml.Name         `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleWithWebIdentityResponse"`
	Result  assumeRoleResult `xml:"AssumeRoleWithWebIdentityResult"`
}

type assumeRoleResult struct {
	Credentials                 credentials `xml:"Credentials"`
	SubjectFromWebIdentityToken string      `xml:"SubjectFromWebIdentityToken"`
}

type credentials struct {
	AccessKeyID     string `xml:"AccessKeyId"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	// SessionToken is always empty, and that is not an omission. The credentials
	// are a real key pair in the backend rather than a token the endpoint signs,
	// so there is nothing for the S3 requests to carry — Garage authenticates them
	// as it authenticates any other SigV4 request. Clients that expect a session
	// token treat an empty one as "none needed", which is the truth.
	SessionToken string `xml:"SessionToken"`
	Expiration   string `xml:"Expiration"`
}

type errorResponse struct {
	XMLName xml.Name  `xml:"https://sts.amazonaws.com/doc/2011-06-15/ ErrorResponse"`
	Error   errorBody `xml:"Error"`
}

type errorBody struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func writeCredentials(w http.ResponseWriter, user authnv1.UserInfo, creds garage.TemporaryCredentials) {
	monitoring.STSRequest("Granted")
	resp := assumeRoleResponse{
		Result: assumeRoleResult{
			Credentials: credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				Expiration:      creds.ExpiresAt.UTC().Format(time.RFC3339),
			},
			SubjectFromWebIdentityToken: user.Username,
		},
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(resp)
}

// writeError is the one funnel every refusal goes through, which is why the
// counter lives here: a consumer that starts being refused — a RoleBinding taken
// away, a client asking for a key on every request until it hits the per-bucket
// cap — leaves no other trace on this side.
func writeError(w http.ResponseWriter, status int, code, message string) {
	monitoring.STSRequest(code)

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(errorResponse{
		Error: errorBody{Type: "Sender", Code: code, Message: message},
	})
}

// --- the API server, as this endpoint uses it -------------------------------

// KubeAuthorizer answers "who is this" and "may they" through the API server.
type KubeAuthorizer struct {
	Client client.Client
}

// Authenticate reviews the token and reports the identity behind it.
//
// The audience is checked by asking for it: a TokenReview with an audience
// returns authenticated=false for a token that was not issued for it, which is
// what keeps this endpoint from accepting the API server's own tokens.
func (a *KubeAuthorizer) Authenticate(ctx context.Context, token string) (authnv1.UserInfo, bool, error) {
	review := &authnv1.TokenReview{
		Spec: authnv1.TokenReviewSpec{
			Token:     token,
			Audiences: []string{TokenAudience},
		},
	}
	if err := a.Client.Create(ctx, review); err != nil {
		return authnv1.UserInfo{}, false, err
	}
	if !review.Status.Authenticated {
		return authnv1.UserInfo{}, false, nil
	}
	return review.Status.User, true, nil
}

// Authorize asks whether the user may get the named subresource of the
// SystemBucket.
//
// It is a SubjectAccessReview rather than any bookkeeping of this module's own,
// so the answer comes from the same RBAC an administrator already reads and
// writes — including the parts this module knows nothing about: group bindings,
// aggregated roles, a policy that grants everything in a namespace.
func (a *KubeAuthorizer) Authorize(ctx context.Context, user authnv1.UserInfo, namespace, name, subresource string) (bool, error) {
	extra := map[string]authzv1.ExtraValue{}
	for k, v := range user.Extra {
		extra[k] = authzv1.ExtraValue(v)
	}
	review := &authzv1.SubjectAccessReview{
		ObjectMeta: metav1.ObjectMeta{},
		Spec: authzv1.SubjectAccessReviewSpec{
			User:   user.Username,
			UID:    user.UID,
			Groups: user.Groups,
			Extra:  extra,
			ResourceAttributes: &authzv1.ResourceAttributes{
				Group:       v1alpha1.APIGroup,
				Version:     v1alpha1.APIVersion,
				Resource:    "systembuckets",
				Subresource: subresource,
				Namespace:   namespace,
				Name:        name,
				Verb:        "get",
			},
		},
	}
	if err := a.Client.Create(ctx, review); err != nil {
		return false, err
	}
	return review.Status.Allowed && !review.Status.Denied, nil
}
