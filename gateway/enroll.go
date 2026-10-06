package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

// maxEnrollBytes bounds an enrollment request's body.
const maxEnrollBytes = 16 << 10

// machineInfo reads what a machine says of itself, as untrusted: a name cut
// and cleaned (the host's if none), known shapes of capabilities only.
func machineInfo(req machine.EnrollRequest) (store.MachineInfo, error) {
	name := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, strings.TrimSpace(req.Name))
	if name == "" {
		name = "machine"
	}
	if len(req.Capabilities) > machine.MaxCapabilities {
		return store.MachineInfo{}, fmt.Errorf("%d capabilities", len(req.Capabilities))
	}
	for _, c := range req.Capabilities {
		if !machine.ValidCapability(c) {
			return store.MachineInfo{}, fmt.Errorf("capability %q", c)
		}
	}
	maxDirectives := req.MaxDirectives
	if maxDirectives < 1 || maxDirectives > machine.MaxDirectives {
		maxDirectives = 1
	}
	return store.MachineInfo{
		Name:          machine.Cut(name, 64),
		OS:            machine.Cut(req.OS, 64),
		Capabilities:  req.Capabilities,
		MaxDirectives: maxDirectives,
		AgentVersion:  machine.Cut(req.AgentVersion, 64),
	}, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (g *Gateway) clientAddr(r *http.Request) string {
	if g.ClientAddr == nil {
		return ""
	}
	return g.ClientAddr(r)
}

// ServeDevice is POST /machines/device: a machine asks to be enrolled. No
// login: it gets a user code for its owner to type in the front, and a
// secret to fetch its token with once approved. Bounded: per client
// address when it is known, and in total.
func (g *Gateway) ServeDevice(w http.ResponseWriter, r *http.Request) {
	var req machine.EnrollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEnrollBytes)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, machine.TokenGrant{Error: machine.ErrorRequest})
		return
	}
	info, err := machineInfo(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, machine.TokenGrant{Error: machine.ErrorRequest})
		return
	}
	addr := g.clientAddr(r)
	ctx := r.Context()
	now := time.Now()
	limited := g.AddrsKnown && addr != ""
	if limited && g.deviceTries.Blocked(addr) {
		writeJSON(w, http.StatusTooManyRequests, machine.TokenGrant{Error: machine.ErrorBusy})
		return
	}
	if n, err := g.Store.CountPendingDeviceRequests(ctx, now); err != nil || n >= maxPendingDevices {
		if err != nil {
			log.Printf("machines: count device requests: %v", err)
		}
		writeJSON(w, http.StatusServiceUnavailable, machine.TokenGrant{Error: machine.ErrorBusy})
		return
	}
	if limited {
		g.deviceTries.Fail(addr) // counts the requests, not failures
	}
	secret := machine.NewDeviceSecret()
	e := store.MachineEnrollment{ID: uuid.NewString(), Kind: store.EnrollmentDevice, SecretHash: machine.HashToken(secret),
		Info: info, ClientAddr: addr, ExpiresAt: now.Add(deviceTTL)}
	for range 5 {
		e.UserCode = machine.NewUserCode()
		if err = g.Store.CreateMachineEnrollment(ctx, e); !errors.Is(err, store.ErrUserCodeTaken) {
			break
		}
	}
	if err != nil {
		log.Printf("machines: create a device request: %v", err)
		writeJSON(w, http.StatusInternalServerError, machine.TokenGrant{Error: machine.ErrorRequest})
		return
	}
	log.Printf("machines: device request %s from %q for %q", e.ID, addr, info.Name)
	writeJSON(w, http.StatusOK, machine.DeviceGrant{
		DeviceCode:       secret,
		UserCode:         machine.FormatUserCode(e.UserCode),
		VerificationPath: machine.VerificationPath,
		ExpiresIn:        int(deviceTTL / time.Second),
		Interval:         int(machine.DefaultPollInterval / time.Second),
	})
}

// ServeDeviceToken is POST /machines/device/token: a waiting machine polls
// with its secret; once its request is approved, it gets its token, once.
func (g *Gateway) ServeDeviceToken(w http.ResponseWriter, r *http.Request) {
	var req machine.TokenRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEnrollBytes)).Decode(&req); err != nil || req.DeviceCode == "" {
		writeJSON(w, http.StatusBadRequest, machine.TokenGrant{Error: machine.ErrorRequest})
		return
	}
	g.redeem(w, r, store.EnrollmentDevice, req.DeviceCode, store.MachineInfo{})
}

// ServeEnroll is POST /machines/enroll: a script enrolls its machine with an
// enrollment token a logged-in user created.
func (g *Gateway) ServeEnroll(w http.ResponseWriter, r *http.Request) {
	var req machine.EnrollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEnrollBytes)).Decode(&req); err != nil || req.EnrollmentToken == "" {
		writeJSON(w, http.StatusBadRequest, machine.TokenGrant{Error: machine.ErrorRequest})
		return
	}
	info, err := machineInfo(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, machine.TokenGrant{Error: machine.ErrorRequest})
		return
	}
	g.redeem(w, r, store.EnrollmentToken, req.EnrollmentToken, info)
}

func (g *Gateway) redeem(w http.ResponseWriter, r *http.Request, kind, secret string, info store.MachineInfo) {
	token := machine.NewMachineToken()
	m, err := g.Store.RedeemMachineEnrollment(r.Context(), kind, machine.HashToken(secret), info,
		store.Machine{ID: uuid.NewString()}, machine.HashToken(token), time.Now())
	switch {
	case errors.Is(err, store.ErrEnrollmentPending):
		writeJSON(w, http.StatusBadRequest, machine.TokenGrant{Error: machine.ErrorPending})
		return
	case errors.Is(err, store.ErrEnrollmentExpired):
		writeJSON(w, http.StatusBadRequest, machine.TokenGrant{Error: machine.ErrorExpired})
		return
	case errors.Is(err, store.ErrEnrollmentUnknown):
		writeJSON(w, http.StatusBadRequest, machine.TokenGrant{Error: machine.ErrorInvalid})
		return
	case err != nil:
		log.Printf("machines: redeem an enrollment: %v", err)
		writeJSON(w, http.StatusInternalServerError, machine.TokenGrant{Error: machine.ErrorRequest})
		return
	}
	log.Printf("machines: machine %s (%s) enrolled for user %s", m.ID, m.Name, m.UserID)
	g.alert(r.Context(), m.UserID, fmt.Sprintf("Nouvelle machine inscrite sur ton compte : « %s » (%s). Si ce n'est pas toi, révoque-la dans « Mes machines ».",
		m.Name, m.OS))
	writeJSON(w, http.StatusOK, machine.TokenGrant{MachineID: m.ID, Name: m.Name, Token: token})
}

// FindRequest returns the device request userID typed the code of, for them
// to approve. Wrong codes are limited per user: an approval page must not
// let anyone try codes until one matches someone's machine.
func (g *Gateway) FindRequest(ctx context.Context, userID, code string) (*store.MachineEnrollment, error) {
	if g.codeTries.Blocked(userID) {
		return nil, ErrTooManyTries
	}
	var e *store.MachineEnrollment
	if norm := machine.NormalizeUserCode(code); norm != "" {
		var err error
		if e, err = g.Store.FindDeviceRequest(ctx, norm, time.Now()); err != nil {
			return nil, err
		}
	}
	if e == nil {
		g.codeTries.Fail(userID)
		return nil, ErrUnknownCode
	}
	return e, nil
}

// Approve gives the device request id, of the code userID typed, to userID:
// at its next poll, the machine gets its token, as theirs.
func (g *Gateway) Approve(ctx context.Context, userID, id, code string) error {
	if g.codeTries.Blocked(userID) {
		return ErrTooManyTries
	}
	norm := machine.NormalizeUserCode(code)
	err := g.Store.ApproveDeviceRequest(ctx, id, norm, userID, time.Now())
	if errors.Is(err, store.ErrEnrollmentUnknown) {
		g.codeTries.Fail(userID)
		return ErrUnknownCode
	}
	if err == nil {
		g.codeTries.Reset(userID)
		log.Printf("machines: device request %s approved by user %s", id, userID)
	}
	return err
}

// CreateEnrollmentToken creates a token that enrolls one machine for
// userID, from a script, within a quarter of an hour.
func (g *Gateway) CreateEnrollmentToken(ctx context.Context, userID string) (string, time.Time, error) {
	token := machine.NewEnrollmentToken()
	expires := time.Now().Add(enrollmentTokenTTL)
	err := g.Store.CreateMachineEnrollment(ctx, store.MachineEnrollment{ID: uuid.NewString(), Kind: store.EnrollmentToken,
		SecretHash: machine.HashToken(token), ApprovedBy: userID, ExpiresAt: expires})
	if err != nil {
		return "", time.Time{}, err
	}
	log.Printf("machines: enrollment token created by user %s", userID)
	return token, expires, nil
}

// Machines returns userID's machines, Online as this gateway sees them.
func (g *Gateway) Machines(ctx context.Context, userID string) ([]MachineView, error) {
	ms, err := g.Store.ListMachines(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]MachineView, len(ms))
	for i, m := range ms {
		out[i] = MachineView{Machine: m, Online: m.RevokedAt == nil && g.Online(m.ID)}
	}
	return out, nil
}

// MachineView is a machine as "Mes machines" shows it.
type MachineView struct {
	store.Machine
	Online bool
}
