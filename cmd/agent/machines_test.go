package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/gateway"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/web/admin"
)

// machineStore is the gateway's store, in memory: what these routes reach.
// Anything else panics (the embedded nil interface).
type machineStore struct {
	gateway.Store
	machines    []store.Machine
	enrollments []store.MachineEnrollment
	revoked     []string
}

func (f *machineStore) ResetMachineConnections(context.Context) error { return nil }
func (f *machineStore) CountPendingDeviceRequests(context.Context, time.Time) (int, error) {
	return len(f.enrollments), nil
}
func (f *machineStore) CreateMachineEnrollment(_ context.Context, e store.MachineEnrollment) error {
	f.enrollments = append(f.enrollments, e)
	return nil
}
func (f *machineStore) FindDeviceRequest(_ context.Context, code string, _ time.Time) (*store.MachineEnrollment, error) {
	for _, e := range f.enrollments {
		if e.UserCode == code && e.ApprovedBy == "" {
			return &e, nil
		}
	}
	return nil, nil
}
func (f *machineStore) ApproveDeviceRequest(_ context.Context, id, code, userID string, _ time.Time) error {
	for i, e := range f.enrollments {
		if e.ID == id && e.UserCode == code && e.ApprovedBy == "" {
			f.enrollments[i].ApprovedBy = userID
			return nil
		}
	}
	return store.ErrEnrollmentUnknown
}
func (f *machineStore) ListMachines(_ context.Context, userID string) ([]store.Machine, error) {
	var out []store.Machine
	for _, m := range f.machines {
		if m.UserID == userID {
			out = append(out, m)
		}
	}
	return out, nil
}
func (f *machineStore) GetMachine(_ context.Context, id string) (*store.Machine, error) {
	for _, m := range f.machines {
		if m.ID == id {
			return &m, nil
		}
	}
	return nil, nil
}
func (f *machineStore) SetMachinePaused(_ context.Context, userID, id string, paused bool) error {
	for i, m := range f.machines {
		if m.ID == id && m.UserID == userID {
			f.machines[i].Paused = paused
			return nil
		}
	}
	return store.ErrMachineNotFound
}
func (f *machineStore) SetMachinePriority(_ context.Context, userID, id string, priority int) error {
	for i, m := range f.machines {
		if m.ID == id && m.UserID == userID {
			f.machines[i].Priority = priority
			return nil
		}
	}
	return store.ErrMachineNotFound
}
func (f *machineStore) RevokeMachine(_ context.Context, id, _ string) ([]store.Directive, error) {
	f.revoked = append(f.revoked, id)
	return nil, nil
}

func newMachinesRouteTest(t *testing.T) (http.Handler, *machineStore) {
	t.Helper()
	auth.LoginFailDelay = 0
	hash, _ := auth.HashPassword(pw)
	st := &routeStore{
		users: []store.User{
			{ID: "u-alice", Email: "alice@example.com", PasswordHash: hash, Role: store.UserRoleStandard},
			{ID: "u-bob", Email: "bob@example.com", PasswordHash: hash, Role: store.UserRoleStandard},
		},
		logins: map[string]string{},
	}
	ms := &machineStore{machines: []store.Machine{
		{ID: "m-alice", UserID: "u-alice", Name: "maison", Capabilities: []string{"echo"}, MaxDirectives: 1},
		{ID: "m-bob", UserID: "u-bob", Name: "bureau-de-bob", MaxDirectives: 1},
	}}
	g := &gateway.Gateway{Store: ms}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := g.Start(ctx); err != nil {
		t.Fatal(err)
	}
	svc := &auth.Service{Store: st}
	srv := newServer(&config.Config{WorkflowQueue: "agent"}, st, nil, sse.NewHub(), svc, admin.New(admin.Config{Auth: svc}).Routes())
	srv.machines = g
	return srv.routes(), ms
}

func postForm(t *testing.T, h http.Handler, path string, form url.Values, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", "http://example.com")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// A machine has no browser and no login: its routes take neither a cookie
// nor an Origin. The pages are a logged-in user's.
func TestMachines_Routes(t *testing.T) {
	h, ms := newMachinesRouteTest(t)

	req := httptest.NewRequest(http.MethodPost, "/machines/device", strings.NewReader(`{"name":"maison","capabilities":["echo"],"max_directives":1}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var grant machine.DeviceGrant
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &grant) != nil || grant.UserCode == "" || grant.DeviceCode == "" {
		t.Fatalf("device request: %d %s", w.Code, w.Body)
	}
	if len(ms.enrollments) != 1 || ms.enrollments[0].SecretHash == grant.DeviceCode || ms.enrollments[0].SecretHash != machine.HashToken(grant.DeviceCode) {
		t.Errorf("stored: %+v", ms.enrollments)
	}
	req = httptest.NewRequest(http.MethodGet, "/machines/connect", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("connect without a token: %d", w.Code)
	}

	if w := call(t, h, http.MethodGet, "/machines", "", nil); w.Code != http.StatusSeeOther || !strings.HasPrefix(w.Header().Get("Location"), "/login") {
		t.Errorf("machines without login: %d", w.Code)
	}
	alice := logIn(t, h, "alice@example.com")
	w = call(t, h, http.MethodGet, "/machines", "", alice)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "maison") || strings.Contains(w.Body.String(), "bureau-de-bob") {
		t.Errorf("alice's machines: %d", w.Code)
	}
	// Pause and priority: her machines only, in bounds.
	if w := postForm(t, h, "/machines/m-alice/pause", url.Values{"paused": {"true"}}, alice); w.Code != http.StatusSeeOther || !ms.machines[0].Paused {
		t.Errorf("pause: %d %+v", w.Code, ms.machines[0])
	}
	if w := postForm(t, h, "/machines/m-bob/pause", url.Values{"paused": {"true"}}, alice); w.Code != http.StatusNotFound || ms.machines[1].Paused {
		t.Errorf("pause bob's: %d", w.Code)
	}
	if w := postForm(t, h, "/machines/m-alice/priority", url.Values{"priority": {"5"}}, alice); w.Code != http.StatusSeeOther || ms.machines[0].Priority != 5 {
		t.Errorf("priority: %d %+v", w.Code, ms.machines[0])
	}
	if w := postForm(t, h, "/machines/m-alice/priority", url.Values{"priority": {"99"}}, alice); w.Code != http.StatusBadRequest {
		t.Errorf("priority out of bounds: %d", w.Code)
	}
	if w := postForm(t, h, "/machines/m-bob/revoke", nil, alice); w.Code != http.StatusNotFound || len(ms.revoked) != 0 {
		t.Errorf("alice revokes bob's machine: %d %v", w.Code, ms.revoked)
	}
	if w := postForm(t, h, "/machines/m-alice/revoke", nil, alice); w.Code != http.StatusSeeOther || len(ms.revoked) != 1 {
		t.Errorf("alice revokes hers: %d %v", w.Code, ms.revoked)
	}

	// An enrollment token: post, redirect, shown once to its user.
	w = postForm(t, h, "/machines/enrollment-token", nil, alice)
	loc := w.Header().Get("Location")
	if w.Code != http.StatusSeeOther || !strings.HasPrefix(loc, "/machines?token=") {
		t.Fatalf("enrollment token: %d %q", w.Code, loc)
	}
	if w := call(t, h, http.MethodGet, loc, "", logIn(t, h, "bob@example.com")); strings.Contains(w.Body.String(), "age_") {
		t.Error("another user's page shows the token")
	}
	if w := call(t, h, http.MethodGet, loc, "", alice); !strings.Contains(w.Body.String(), "age_") || w.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("token page: %d", w.Code)
	}
	if w := call(t, h, http.MethodGet, loc, "", alice); strings.Contains(w.Body.String(), "age_") || !strings.Contains(w.Body.String(), "qu&#39;une fois") {
		t.Error("a reload shows the token again")
	}

	// The code typed: the request it designates is shown, with its warning.
	w = postForm(t, h, "/machines/activer", url.Values{"code": {strings.ToLower(grant.UserCode)}}, alice)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "maison") || !strings.Contains(w.Body.String(), "propre machine") {
		t.Errorf("activation: %d %s", w.Code, w.Body)
	}
	if w := postForm(t, h, "/machines/activer/approve", url.Values{"request": {ms.enrollments[0].ID}, "code": {grant.UserCode}}, alice); w.Code != http.StatusSeeOther ||
		ms.enrollments[0].ApprovedBy != "u-alice" {
		t.Errorf("approve: %d %+v", w.Code, ms.enrollments[0])
	}
	// Wrong codes are limited.
	for i := range 11 {
		w = postForm(t, h, "/machines/activer", url.Values{"code": {"AAA-AAA"}}, alice)
		if want := map[bool]int{true: http.StatusTooManyRequests, false: http.StatusNotFound}[i == 10]; w.Code != want {
			t.Fatalf("try %d: %d, want %d", i+1, w.Code, want)
		}
	}
	if w := postForm(t, h, "/machines/activer", url.Values{"code": {grant.UserCode}}, alice); w.Code != http.StatusTooManyRequests {
		t.Errorf("the right code past the limit: %d", w.Code)
	}
	// Another user is not limited by alice's tries; an approved request is
	// found no more.
	bob := logIn(t, h, "bob@example.com")
	if w := postForm(t, h, "/machines/activer", url.Values{"code": {grant.UserCode}}, bob); w.Code != http.StatusNotFound {
		t.Errorf("bob: %d", w.Code)
	}
}
