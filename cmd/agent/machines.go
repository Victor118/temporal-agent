package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/gateway"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/web/chat"
	"github.com/victor/temporal-agent/workflow"
)

// newGateway starts the machines' gateway of a server (docs/design/
// machines.md): one per process, the only replica. It speaks to Temporal as
// a client, and tells a machine's owner what they must know through their
// notifications.
func newGateway(cfg *config.Config, st *store.PostgresStore, tc client.Client, hub *sse.Hub) *gateway.Gateway {
	clients, err := auth.ParseClientAddrs(cfg.TrustedProxies)
	if err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}
	if _, err := cfg.MachinesOn(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}
	g := &gateway.Gateway{Store: st, Temporal: tc, Alert: machineAlerts(st, hub), ClientAddr: clients.Of, AddrsKnown: clients.Known(),
		Notice: machineNotices(hub)}
	if err := g.Start(context.Background()); err != nil {
		log.Fatalf("Machines gateway: %v", err)
	}
	return g
}

// machineNotices shows what a directive does on its turn's line: a notice
// event on the session's topic, which the server reads in passing
// (session.Service.Observe, the participant's note) and the pages reload
// on. Web only.
func machineNotices(hub *sse.Hub) func(sessionID, participant, agent, text string) {
	return func(sessionID, participant, agent, text string) {
		data, _ := json.Marshal(map[string]string{"type": activity.EventNotice, "text": text, "agent": agent, "participant": participant})
		hub.Publish(sessionID, activity.SSEEvent{Type: activity.EventNotice, Data: data})
	}
}

// notificationAppender stores a notification.
type notificationAppender interface {
	AppendMessage(ctx context.Context, sessionID, key string, msg store.Message) (int64, error)
}

// machineAlerts posts a machine's alert in its owner's notifications, and
// shows it live, like a scheduled task's result.
func machineAlerts(st notificationAppender, hub *sse.Hub) func(ctx context.Context, userID, text string) {
	return func(ctx context.Context, userID, text string) {
		topic := notificationsOf(userID)
		if _, err := st.AppendMessage(ctx, topic, "machine:"+newUUID(), store.Message{Role: store.RoleAssistant, Content: text}); err != nil {
			log.Printf("machines: notify user %s: %v", userID, err)
			return
		}
		data, _ := json.Marshal(text)
		hub.Publish(topic, activity.SSEEvent{Type: "notification", Data: data})
	}
}

// machinesUI is "Mes machines" and the approval of a machine's code: a
// logged-in user's own machines only. The rules are the gateway's.
type machinesUI struct {
	gw *gateway.Gateway

	mu sync.Mutex
	// tokens are the enrollment tokens just created, waiting for the one
	// page that shows them, by a key of their own (post, redirect, get: a
	// reload never creates a second token, nor shows this one again).
	tokens map[string]shownToken
}

// shownToken is an enrollment token on its way to its page.
type shownToken struct {
	userID  string
	token   string
	expires time.Time // the token's own expiry
	showBy  time.Time // past it, the page shows it no more
}

// tokenShowWindow is how long a created token waits for its page.
const tokenShowWindow = time.Minute

// keep holds a token for its page, and returns the key the page asks it by.
func (m *machinesUI) keep(t shownToken) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tokens == nil {
		m.tokens = map[string]shownToken{}
	}
	now := time.Now()
	for k, old := range m.tokens {
		if now.After(old.showBy) {
			delete(m.tokens, k)
		}
	}
	key := newUUID()
	t.showBy = now.Add(tokenShowWindow)
	m.tokens[key] = t
	return key
}

// take gives a kept token to its user's page, once.
func (m *machinesUI) take(key, userID string) (shownToken, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tokens[key]
	if !ok || t.userID != userID {
		return shownToken{}, false
	}
	delete(m.tokens, key)
	return t, time.Now().Before(t.showBy)
}

// serverURL is the server's address as the browser reached it, for the
// commands the page shows.
func serverURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func (m *machinesUI) page(w http.ResponseWriter, r *http.Request, p chat.MachinesPage, status int) {
	me := auth.UserFrom(r.Context())
	views, err := m.gw.Machines(r.Context(), me.ID)
	if err != nil {
		log.Printf("ui: machines of %s: %v", me.ID, err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	p.Me = chat.UserPerson(me)
	p.Server = serverURL(r)
	for _, v := range views {
		row := chat.MachineRow{ID: v.ID, Name: v.Name, OS: v.OS, Capabilities: slices.Clone(v.Capabilities), Online: v.Online,
			OpenDirectives: v.OpenDirectives, MaxDirectives: v.MaxDirectives, CreatedAt: v.CreatedAt, Revoked: v.RevokedAt != nil,
			RevokedReason: v.RevokedReason, LastAddr: v.LastAddr, AgentVersion: v.AgentVersion,
			Paused: v.Paused, Priority: v.Priority, ClaudeCode: v.ClaudeCode, OpenKinds: v.OpenKinds}
		if v.SeenAt != nil {
			row.SeenAt = *v.SeenAt
		}
		p.Machines = append(p.Machines, row)
	}
	// An enrollment token is shown once: never cached.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	chat.Render(w, "machines", p)
}

func (m *machinesUI) list(w http.ResponseWriter, r *http.Request) {
	var p chat.MachinesPage
	switch r.URL.Query().Get("ok") {
	case "approved":
		p.Flash = "Machine approuvée : elle reçoit son jeton et se connecte d'elle-même."
	case "revoked":
		p.Flash = "Machine révoquée."
	case "paused":
		p.Flash = "Machine mise en pause : elle ne reçoit plus rien."
	case "resumed":
		p.Flash = "Machine reprise."
	case "priority":
		p.Flash = "Priorité enregistrée."
	}
	if key := r.URL.Query().Get("token"); key != "" {
		if t, ok := m.take(key, auth.UserFrom(r.Context()).ID); ok {
			p.EnrollmentToken, p.TokenExpires = t.token, t.expires
		} else {
			p.Flash = "Un jeton d'inscription n'est affiché qu'une fois : crée-en un autre au besoin."
		}
	}
	m.page(w, r, p, http.StatusOK)
}

func (m *machinesUI) enrollmentToken(w http.ResponseWriter, r *http.Request) {
	me := auth.UserFrom(r.Context())
	token, expires, err := m.gw.CreateEnrollmentToken(r.Context(), me.ID)
	if err != nil {
		log.Printf("ui: enrollment token: %v", err)
		m.page(w, r, chat.MachinesPage{Error: "Erreur interne."}, http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/machines?token="+m.keep(shownToken{userID: me.ID, token: token, expires: expires}))
}

func (m *machinesUI) revoke(w http.ResponseWriter, r *http.Request) {
	err := m.gw.Revoke(r.Context(), auth.UserFrom(r.Context()).ID, chi.URLParam(r, "machineID"))
	switch {
	case errors.Is(err, gateway.ErrMachineNotFound):
		http.Error(w, "Not found", http.StatusNotFound)
	case err != nil:
		log.Printf("ui: revoke: %v", err)
		m.page(w, r, chat.MachinesPage{Error: "Erreur interne."}, http.StatusInternalServerError)
	default:
		goTo(w, r, "/machines?ok=revoked")
	}
}

func (m *machinesUI) pause(w http.ResponseWriter, r *http.Request) {
	paused := r.FormValue("paused") == "true"
	err := m.gw.SetPaused(r.Context(), auth.UserFrom(r.Context()).ID, chi.URLParam(r, "machineID"), paused)
	switch {
	case errors.Is(err, gateway.ErrMachineNotFound):
		http.Error(w, "Not found", http.StatusNotFound)
	case err != nil:
		log.Printf("ui: pause: %v", err)
		m.page(w, r, chat.MachinesPage{Error: "Erreur interne."}, http.StatusInternalServerError)
	case paused:
		goTo(w, r, "/machines?ok=paused")
	default:
		goTo(w, r, "/machines?ok=resumed")
	}
}

func (m *machinesUI) priority(w http.ResponseWriter, r *http.Request) {
	prio, err := strconv.Atoi(r.FormValue("priority"))
	if err != nil || prio < gateway.MinPriority || prio > gateway.MaxPriority {
		m.page(w, r, chat.MachinesPage{Error: fmt.Sprintf("Priorité : un nombre entre %d et %d.", gateway.MinPriority, gateway.MaxPriority)},
			http.StatusBadRequest)
		return
	}
	err = m.gw.SetPriority(r.Context(), auth.UserFrom(r.Context()).ID, chi.URLParam(r, "machineID"), prio)
	switch {
	case errors.Is(err, gateway.ErrMachineNotFound):
		http.Error(w, "Not found", http.StatusNotFound)
	case err != nil:
		log.Printf("ui: priority: %v", err)
		m.page(w, r, chat.MachinesPage{Error: "Erreur interne."}, http.StatusInternalServerError)
	default:
		goTo(w, r, "/machines?ok=priority")
	}
}

func (m *machinesUI) activatePage(w http.ResponseWriter, r *http.Request) {
	chat.Render(w, "machine-activate", chat.MachineActivatePage{Me: chat.UserPerson(auth.UserFrom(r.Context()))})
}

// activateError answers a code that leads nowhere.
func activateError(err error) (string, int) {
	switch {
	case errors.Is(err, gateway.ErrTooManyTries):
		return "Trop d'essais : réessaie dans quelques minutes.", http.StatusTooManyRequests
	case errors.Is(err, gateway.ErrUnknownCode):
		return "Code inconnu ou expiré : vérifie celui qu'affiche ta machine.", http.StatusNotFound
	}
	log.Printf("ui: machine code: %v", err)
	return "Erreur interne.", http.StatusInternalServerError
}

// activate shows the request a code designates, for its user to approve.
func (m *machinesUI) activate(w http.ResponseWriter, r *http.Request) {
	me := auth.UserFrom(r.Context())
	p := chat.MachineActivatePage{Me: chat.UserPerson(me), Code: r.FormValue("code")}
	req, err := m.gw.FindRequest(r.Context(), me.ID, p.Code)
	if err != nil {
		var status int
		p.Error, status = activateError(err)
		w.WriteHeader(status)
		chat.Render(w, "machine-activate", p)
		return
	}
	p.Request = &chat.MachineRequest{ID: req.ID, Code: p.Code, Name: req.Info.Name, OS: req.Info.OS,
		Capabilities: req.Info.Capabilities, ClientAddr: req.ClientAddr, CreatedAt: req.CreatedAt, ExpiresAt: req.ExpiresAt}
	chat.Render(w, "machine-activate", p)
}

func (m *machinesUI) approve(w http.ResponseWriter, r *http.Request) {
	me := auth.UserFrom(r.Context())
	err := m.gw.Approve(r.Context(), me.ID, r.FormValue("request"), r.FormValue("code"))
	if err != nil {
		p := chat.MachineActivatePage{Me: chat.UserPerson(me)}
		var status int
		p.Error, status = activateError(err)
		w.WriteHeader(status)
		chat.Render(w, "machine-activate", p)
		return
	}
	goTo(w, r, "/machines?ok=approved")
}

// machineEchoCmd starts an echo directive on a user's machine and waits for
// it: what tries the machines by hand before a tool sends them anything
// (phase 0). It needs a worker on the workflow queue (agent dev or agent
// worker) and the server's gateway.
var machineEchoCmd = &cobra.Command{
	Use:   "machine-echo",
	Short: "Run an echo directive on one of a user's machines (to try the machines by hand)",
	Run:   runMachineEcho,
}

func init() {
	f := machineEchoCmd.Flags()
	f.String("email", "", "the user whose machine runs it (required)")
	f.String("text", "bonjour", "what comes back")
	f.Duration("duration", 10*time.Second, "how long the echo takes")
	f.Duration("progress-every", time.Second, "how often the machine says how far it is (0 = never)")
	machineEchoCmd.MarkFlagRequired("email")
}

func runMachineEcho(cmd *cobra.Command, args []string) {
	email, _ := cmd.Flags().GetString("email")
	text, _ := cmd.Flags().GetString("text")
	d, _ := cmd.Flags().GetDuration("duration")
	every, _ := cmd.Flags().GetDuration("progress-every")
	cfg := config.Load()
	st := openStore(cfg)
	defer st.Close()
	ctx := context.Background()
	u, err := st.GetUserByEmail(ctx, email)
	if err != nil || u == nil {
		log.Fatalf("No user %s (%v)", email, err)
	}
	tc := dialTemporal(cfg)
	defer tc.Close()
	run, err := tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "machine-echo-" + newUUID(), TaskQueue: cfg.WorkflowQueue},
		workflow.MachineEchoWorkflow, workflow.MachineEchoInput{UserID: u.ID, Text: text, Duration: d, ProgressEvery: every})
	if err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Printf("Workflow %s started, waiting for it…\n", run.GetID())
	var out workflow.MachineEchoOutput
	if err := run.Get(ctx, &out); err != nil {
		log.Fatalf("Echo failed: %v", err)
	}
	if out.NoMachine != "" {
		fmt.Println("No machine:", out.NoMachine)
		return
	}
	fmt.Printf("Machine %q answered %q (%d progresses, last %q)\n", out.Machine, out.Text, out.Progresses, out.Progress)
}
