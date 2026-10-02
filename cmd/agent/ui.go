package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/web/chat"
	"github.com/victor/temporal-agent/workflow"
)

// The HTML interface: server-rendered pages and the fragments htmx swaps in.
// The session's SSE stream only rings the bell; the fragments carry the state.

// --- Access ---

// requireUserPage sends a visitor without a login session to the login page.
func (h *handler) requireUserPage(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, err := h.auth.UserFromRequest(r)
		if err != nil {
			log.Printf("ui: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if u == nil {
			login := "/login?next=" + url.QueryEscape(r.URL.RequestURI())
			if r.Header.Get("HX-Request") != "" {
				w.Header().Set("HX-Redirect", login)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, login, http.StatusSeeOther)
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), u)))
	})
}

func (h *handler) loginPage(w http.ResponseWriter, r *http.Request) {
	if u, _ := h.auth.UserFromRequest(r); u != nil {
		http.Redirect(w, r, safeLocal(r.URL.Query().Get("next")), http.StatusSeeOther)
		return
	}
	chat.Render(w, "login", chat.LoginPage{Next: r.URL.Query().Get("next")})
}

func (h *handler) loginForm(w http.ResponseWriter, r *http.Request) {
	page := chat.LoginPage{Email: r.FormValue("email"), Next: r.FormValue("next")}
	token, _, err := h.auth.Login(r.Context(), auth.ClientAddr(r), page.Email, r.FormValue("password"))
	status := http.StatusUnauthorized
	switch {
	case errors.Is(err, auth.ErrInvalidCredentials):
		page.Error = "Email ou mot de passe incorrect."
	case errors.Is(err, auth.ErrTooManyAttempts):
		page.Error = "Trop de tentatives : réessaie dans quelques minutes."
		status = http.StatusTooManyRequests
	case err != nil:
		log.Printf("ui: login: %v", err)
		page.Error = "Erreur interne."
	default:
		auth.SetCookie(w, r, token)
		http.Redirect(w, r, safeLocal(page.Next), http.StatusSeeOther)
		return
	}
	w.WriteHeader(status)
	chat.Render(w, "login", page)
}

func (h *handler) logoutForm(w http.ResponseWriter, r *http.Request) {
	h.auth.Logout(r.Context(), r)
	auth.ClearCookie(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// safeLocal keeps a post-login redirect on this site.
func safeLocal(next string) string {
	if strings.HasPrefix(next, "/") && !strings.HasPrefix(next, "//") && !strings.Contains(next, `\`) {
		return next
	}
	return "/"
}

// goTo sends the browser to a page after a form post: through htmx when the
// form was boosted, through a plain redirect otherwise.
func goTo(w http.ResponseWriter, r *http.Request, path string) {
	if r.Header.Get("HX-Request") != "" {
		w.Header().Set("HX-Location", path)
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, path, http.StatusSeeOther)
}

// --- Session states, from Temporal ---

// sessionStatuses tells what each session is doing, from the workflows
// running for it: a question waiting, an agent turn, or the session's own
// workflow waiting for messages. Three visibility queries, whatever the number
// of sessions. A failed query degrades the states shown, nothing else.
func (h *handler) sessionStatuses(ctx context.Context) map[string]chat.Status {
	statuses := map[string]chat.Status{}
	mark := func(workflowType string, status chat.Status, sessionOf func(id string) string) {
		resp, err := h.temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: h.cfg.TemporalNamespace,
			Query:     fmt.Sprintf("WorkflowType = '%s' AND ExecutionStatus = 'Running'", workflowType),
			PageSize:  1000,
		})
		if err != nil {
			log.Printf("ui: list running %s: %v", workflowType, err)
			return
		}
		for _, e := range resp.Executions {
			if sid := sessionOf(e.Execution.WorkflowId); sid != "" {
				statuses[sid] = statuses[sid].Stronger(status)
			}
		}
	}
	// "session-<id>" or "session-<id>-<unix time>" for a resumed run.
	mark("SessionWorkflow", chat.StatusActive, func(id string) string { return uuidPrefix(strings.TrimPrefix(id, "session-")) })
	// "<id>-turn-<n>", and sub-agents "<id>-tool-…".
	mark("AgentWorkflow", chat.StatusWorking, uuidPrefix)
	// A fork's summary being written.
	mark("ForkSessionWorkflow", chat.StatusWorking, func(id string) string { return uuidPrefix(strings.TrimPrefix(id, "fork-")) })
	// "<id>-tool-ask_user-…", from the session's agent or a sub-agent of it.
	mark("AskUserWorkflow", chat.StatusWaiting, uuidPrefix)
	return statuses
}

// uuidPrefix returns the session ID (a UUID, 36 characters) a workflow ID
// starts with, or "".
func uuidPrefix(id string) string {
	if len(id) < 36 || (len(id) > 36 && id[36] != '-') {
		return ""
	}
	return id[:36]
}

// pendingQuestions returns the questions the session's agents wait on.
func (h *handler) pendingQuestions(ctx context.Context, sessionID string) []chat.Question {
	query, err := pendingQuestionsQuery(sessionID)
	if err != nil {
		return nil
	}
	resp, err := h.temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: h.cfg.TemporalNamespace,
		Query:     query,
		PageSize:  50,
	})
	if err != nil {
		log.Printf("ui: list questions of %s: %v", sessionID, err)
		return nil
	}
	var out []chat.Question
	for _, e := range resp.Executions {
		id := e.Execution.WorkflowId
		v, err := h.temporalClient.QueryWorkflow(ctx, id, "", workflow.QueryQuestion)
		if err != nil {
			log.Printf("ui: question %s: %v", id, err)
			continue
		}
		var q workflow.PendingQuestion
		if err := v.Get(&q); err != nil {
			continue
		}
		out = append(out, chat.Question{WorkflowID: id, Text: q.Question, AgentChain: q.AgentChain})
	}
	return out
}

// --- Pages ---

// buildPage gathers what the interface shows for a session (or for none: the
// welcome page), as the user may see it.
func (h *handler) buildPage(ctx context.Context, me *store.User, sessionID, view string) (*chat.Page, error) {
	sessions, err := h.store.ListSessionsByUser(ctx, me.ID)
	if err != nil {
		return nil, err
	}
	stats, err := h.store.ListSessionStats(ctx, me.ID)
	if err != nil {
		return nil, err
	}
	statuses := h.sessionStatuses(ctx)
	p := &chat.Page{Me: chat.UserPerson(me), IsAdmin: me.IsAdmin(), View: view}
	p.Roots = chat.BuildTree(sessions, stats, statuses, sessionID)
	if notes, err := h.store.LoadMessagesWithID(ctx, "notifications:"+me.ID); err == nil {
		p.Notifications = len(notes)
	}
	if sessionID == "" {
		return p, nil
	}

	p.Node = chat.Find(p.Roots, sessionID)
	if p.Node == nil {
		return nil, errSessionGone
	}
	p.Crumbs = chat.Path(p.Node)
	sess := p.Node.Session
	p.IsCreator = sess.CreatedBy == me.ID
	p.AgentMode = sess.AgentMode
	if p.AgentMode == "" {
		p.AgentMode = store.AgentModeAuto
	}

	members, err := h.store.ListSessionMembers(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	for _, m := range members {
		name := m.DisplayName
		if name == "" {
			name = m.Email
		}
		p.Members = append(p.Members, chat.Member{Person: chat.NewPerson(m.UserID, name), Email: m.Email})
	}
	p.AgentOnMention = p.AgentMode == store.AgentModeMention || (p.AgentMode == store.AgentModeAuto && len(members) > 1)

	p.Agent = chat.AgentInfo{ID: sess.AgentID, Name: sess.AgentID}
	if a, err := h.store.GetAgent(ctx, sess.AgentID); err == nil && a != nil {
		p.Agent.Name, p.Agent.Description = a.Name, a.Description
	}

	if sess.ParentSessionID != "" {
		p.Parent = &chat.ParentInfo{MessageID: sess.ForkedAtMessageID}
		if ok, _ := h.store.IsSessionMember(ctx, sess.ParentSessionID, me.ID); ok {
			if parent, _ := h.store.GetSession(ctx, sess.ParentSessionID); parent != nil {
				p.Parent.Accessible, p.Parent.SessionID, p.Parent.Title = true, parent.SessionID, parent.Title
			}
		}
	}
	if sess.ForkedAtMessageID != 0 {
		state, err := h.forkSummaryState(ctx, &sess)
		if err != nil {
			return nil, err
		}
		p.SummaryPending, p.SummaryFailed = state == summaryPending, state == summaryFailed
	}

	if view == "map" {
		m := chat.BuildMap(chat.Root(p.Node))
		p.Map = &m
		return p, nil
	}

	msgs, err := h.store.LoadMessagesWithID(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	forks, err := h.store.ListForks(ctx, sessionID, me.ID)
	if err != nil {
		return nil, err
	}
	byMessage := map[int64][]chat.ForkLink{}
	for _, f := range forks {
		title := f.Title
		if title == "" {
			title = "Fork"
		}
		byMessage[f.ForkedAtMessageID] = append(byMessage[f.ForkedAtMessageID], chat.ForkLink{SessionID: f.SessionID, Title: title})
	}
	p.Thread = chat.BuildThread(msgs, me.ID, byMessage, h.pendingQuestions(ctx, sessionID))
	for _, it := range p.Thread {
		if it.Kind == chat.ItemHuman || it.Kind == chat.ItemAgent {
			p.LastMessageID = it.ID
		}
	}
	// Working is an agent turn: a fork's summary workflow finishing up is not.
	p.Working = statuses[sessionID] == chat.StatusWorking && !p.SummaryPending &&
		!(sess.ForkedAtMessageID != 0 && h.isWorkflowRunning(ctx, workflow.ForkWorkflowID(sessionID)))
	return p, nil
}

// renderPage renders a page or one of its fragments, or the error that kept
// it from being built.
func (h *handler) renderPage(w http.ResponseWriter, r *http.Request, sessionID, view, block string, adjust func(*chat.Page)) {
	p, err := h.buildPage(r.Context(), auth.UserFrom(r.Context()), sessionID, view)
	if errors.Is(err, errSessionGone) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		log.Printf("ui: build page %s: %v", sessionID, err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if adjust != nil {
		adjust(p)
	}
	p.Fragment = block == "thread"
	chat.Render(w, block, p)
}

// home opens the session most recently active, or the welcome page.
func (h *handler) home(w http.ResponseWriter, r *http.Request) {
	me := auth.UserFrom(r.Context())
	stats, err := h.store.ListSessionStats(r.Context(), me.ID)
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	latest := ""
	for id, st := range stats {
		if latest == "" || st.LastActivity.After(stats[latest].LastActivity) {
			latest = id
		}
	}
	if latest != "" {
		http.Redirect(w, r, "/s/"+latest, http.StatusSeeOther)
		return
	}
	h.renderPage(w, r, "", "thread", "page", nil)
}

func (h *handler) sessionPage(w http.ResponseWriter, r *http.Request) {
	h.renderPage(w, r, chi.URLParam(r, "id"), "thread", "page", nil)
}

func (h *handler) mapPage(w http.ResponseWriter, r *http.Request) {
	h.renderPage(w, r, chi.URLParam(r, "id"), "map", "page", nil)
}

func (h *handler) threadFragment(w http.ResponseWriter, r *http.Request) {
	h.renderPage(w, r, chi.URLParam(r, "id"), "thread", "thread", nil)
}

// treeFragment refreshes the tree, which the user sees from any page.
func (h *handler) treeFragment(w http.ResponseWriter, r *http.Request) {
	current := r.URL.Query().Get("current")
	me := auth.UserFrom(r.Context())
	sessions, err := h.store.ListSessionsByUser(r.Context(), me.ID)
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	stats, err := h.store.ListSessionStats(r.Context(), me.ID)
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	chat.Render(w, "tree-items", &chat.Page{Roots: chat.BuildTree(sessions, stats, h.sessionStatuses(r.Context()), current)})
}

// --- Actions ---

func (h *handler) newSessionForm(w http.ResponseWriter, r *http.Request) {
	id, err := h.openSession(r.Context(), auth.UserFrom(r.Context()), "", "", "")
	if err != nil {
		log.Printf("ui: new session: %v", err)
		http.Error(w, "Impossible de créer la session", http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/s/"+id)
}

// sendForm posts a member's message and answers with the refreshed thread.
func (h *handler) sendForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	text := r.FormValue("content")
	fail := func(msg string) {
		h.renderPage(w, r, sessionID, "thread", "thread", func(p *chat.Page) { p.Error = msg })
	}
	if strings.TrimSpace(text) == "" {
		fail("Le message est vide.")
		return
	}
	sess, err := h.store.GetSession(r.Context(), sessionID)
	if err != nil || sess == nil {
		http.NotFound(w, r)
		return
	}
	if sess.ForkedAtMessageID != 0 {
		if state, err := h.forkSummaryState(r.Context(), sess); err == nil && state == summaryPending {
			fail("Le résumé de la session parente est en cours : patiente un instant.")
			return
		}
	}
	if _, err := h.deliverMessage(r.Context(), sess, auth.UserFrom(r.Context()), text); err != nil {
		log.Printf("ui: send to %s: %v", sessionID, err)
		fail("Message non envoyé : " + err.Error())
		return
	}
	h.renderPage(w, r, sessionID, "thread", "thread", nil)
}

func (h *handler) forkForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	messageID, _ := strconv.ParseInt(r.FormValue("message_id"), 10, 64)
	f, err := h.fork(r.Context(), sessionID, messageID, auth.UserFrom(r.Context()))
	if err != nil {
		log.Printf("ui: fork %s at %d: %v", sessionID, messageID, err)
		h.renderPage(w, r, sessionID, "thread", "thread", func(p *chat.Page) { p.Error = "Fork impossible : " + err.Error() })
		return
	}
	goTo(w, r, "/s/"+f.SessionID)
}

func (h *handler) inviteForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	err := h.inviteByEmail(r.Context(), sessionID, strings.TrimSpace(r.FormValue("email")), auth.UserFrom(r.Context()).ID)
	if err != nil {
		msg := "Invitation impossible."
		if errors.Is(err, errNoSuchUser) {
			msg = "Aucun compte actif avec cet email."
		}
		h.renderPage(w, r, sessionID, "thread", "rail", func(p *chat.Page) { p.Error = msg })
		return
	}
	// The members show in the top bar and change the agent's default mode:
	// reload the page rather than the rail alone.
	goTo(w, r, "/s/"+sessionID)
}

func (h *handler) agentModeForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	mode := r.FormValue("mode")
	switch mode {
	case store.AgentModeAuto, store.AgentModeAlways, store.AgentModeMention:
	default:
		http.Error(w, "Mode inconnu", http.StatusBadRequest)
		return
	}
	if err := h.store.SetSessionAgentMode(r.Context(), sessionID, mode); err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/s/"+sessionID) // the composer's hint changes with the mode
}

func (h *handler) leaveForm(w http.ResponseWriter, r *http.Request) {
	if err := h.leave(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context()).ID); err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/")
}

func (h *handler) deleteForm(w http.ResponseWriter, r *http.Request) {
	switch err := h.removeSession(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context()).ID); {
	case errors.Is(err, errNotCreator):
		http.Error(w, "Seul le créateur de la session peut la supprimer.", http.StatusForbidden)
	case err != nil:
		http.Error(w, "Internal error", http.StatusInternalServerError)
	default:
		goTo(w, r, "/")
	}
}

// answerForm answers a question of the session in the URL, by any member.
func (h *handler) answerForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	workflowID, answer := r.FormValue("workflow_id"), strings.TrimSpace(r.FormValue("answer"))
	if answer == "" || !strings.HasPrefix(workflowID, sessionID+"-") {
		http.Error(w, "Réponse invalide", http.StatusBadRequest)
		return
	}
	if err := h.temporalClient.SignalWorkflow(r.Context(), workflowID, "", workflow.SignalUserAnswer, answer); err != nil {
		log.Printf("ui: answer %s: %v", workflowID, err)
		h.renderPage(w, r, sessionID, "thread", "thread", func(p *chat.Page) { p.Error = "Réponse non transmise." })
		return
	}
	h.renderPage(w, r, sessionID, "thread", "thread", nil)
}

func (h *handler) cancelForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	if wf := h.findActiveWorkflowID(r.Context(), sessionID); wf != "" {
		if err := h.temporalClient.SignalWorkflow(r.Context(), wf, "", workflow.SignalCancelAgent, nil); err != nil {
			log.Printf("ui: cancel %s: %v", sessionID, err)
		}
	}
	h.renderPage(w, r, sessionID, "thread", "thread", nil)
}

// --- Notifications ---

func (h *handler) notificationsPage(w http.ResponseWriter, r *http.Request) {
	me := auth.UserFrom(r.Context())
	msgs, err := h.store.LoadMessagesWithID(r.Context(), "notifications:"+me.ID)
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	page := chat.NotificationsPage{Me: chat.UserPerson(me)}
	for i := len(msgs) - 1; i >= 0; i-- { // newest first
		page.Items = append(page.Items, chat.Notification{ID: msgs[i].ID, HTML: chat.Markdown(decodeContent(msgs[i].Content))})
	}
	chat.Render(w, "notifications", page)
}

func (h *handler) deleteNotificationForm(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "notifID"), 10, 64)
	if err == nil {
		err = h.store.DeleteMessage(r.Context(), "notifications:"+auth.UserFrom(r.Context()).ID, id)
	}
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/notifications")
}

// decodeContent returns a stored message's text: it is kept as a JSON string.
func decodeContent(content string) string {
	var s string
	if json.Unmarshal([]byte(content), &s) == nil {
		return s
	}
	return content
}
