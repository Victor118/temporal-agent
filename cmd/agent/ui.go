package main

import (
	"cmp"
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/web/chat"
)

// ui is the HTML interface: server-rendered pages and the fragments htmx
// swaps in. The session's SSE stream only rings the bell; the fragments carry
// the state. Like the JSON API, it changes sessions through the session
// service and reads the store only to show it.
type ui struct {
	auth     *auth.Service
	sessions *session.Service
	store    readStore
	// hub tells where a page stands in its streams: the page connects from
	// there, and is sent what was published while it loaded.
	hub *sse.Hub
}

// --- Access ---

// requireUserPage sends a visitor without a login session to the login page.
func (u *ui) requireUserPage(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := u.auth.UserFromRequest(r)
		if err != nil {
			log.Printf("ui: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if user == nil {
			login := "/login?next=" + url.QueryEscape(r.URL.RequestURI())
			if r.Header.Get("HX-Request") != "" {
				w.Header().Set("HX-Redirect", login)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, login, http.StatusSeeOther)
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), user)))
	})
}

func (u *ui) loginPage(w http.ResponseWriter, r *http.Request) {
	if user, _ := u.auth.UserFromRequest(r); user != nil {
		http.Redirect(w, r, safeLocal(r.URL.Query().Get("next")), http.StatusSeeOther)
		return
	}
	chat.Render(w, "login", chat.LoginPage{Next: r.URL.Query().Get("next")})
}

func (u *ui) loginForm(w http.ResponseWriter, r *http.Request) {
	page := chat.LoginPage{Email: r.FormValue("email"), Next: r.FormValue("next")}
	token, _, err := u.auth.Login(r.Context(), u.auth.ClientAddr(r), page.Email, r.FormValue("password"))
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

func (u *ui) logoutForm(w http.ResponseWriter, r *http.Request) {
	u.auth.Logout(r.Context(), r)
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

// --- Pages ---

// buildPage gathers what the interface shows for a session (or for none: the
// welcome page), as the user may see it.
//
// block is what will be rendered of it: the session's files are read only
// for the thread ("page", "thread").
func (u *ui) buildPage(ctx context.Context, me *store.User, sessionID, view, block string) (*chat.Page, error) {
	// Before anything is read: an event published while the page loads is
	// sent again when its stream connects, never lost.
	streamFrom := u.hub.Position(pageTopics(me, sessionID)...)
	sessions, err := u.store.ListSessionsByUser(ctx, me.ID)
	if err != nil {
		return nil, err
	}
	stats, err := u.store.ListSessionStats(ctx, me.ID)
	if err != nil {
		return nil, err
	}
	statuses := u.sessions.Statuses(ctx)
	p := &chat.Page{Me: chat.UserPerson(me), IsAdmin: me.IsAdmin(), View: view, StreamFrom: streamFrom}
	p.Roots = chat.BuildTree(sessions, stats, statuses, sessionID)
	if notes, err := u.store.LoadMessagesWithID(ctx, notificationsOf(me.ID)); err == nil {
		p.Notifications = len(notes)
	}
	if sessionID == "" {
		return p, nil
	}

	p.Node = chat.Find(p.Roots, sessionID)
	if p.Node == nil {
		return nil, session.ErrNotFound
	}
	p.Crumbs = chat.Path(p.Node)
	sess := p.Node.Session
	p.IsCreator = sess.CreatedBy == me.ID
	p.AgentMode = sess.AgentMode
	if p.AgentMode == "" {
		p.AgentMode = store.AgentModeAuto
	}

	members, err := u.store.ListSessionMembers(ctx, sessionID)
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

	// Every agent can be called in a session: the rail lists their mentions,
	// and the thread signs each answer with its agent as it is now.
	directory, err := u.agentDirectory(ctx, &sess)
	if err != nil {
		return nil, err
	}
	p.Agents, p.Agent = directory.List, directory.Session
	// Where each agent stands, and what the viewer may stop: the Agents
	// panel, in the rail of both views.
	p.Participants = u.agentsPanel(ctx, me, &sess, directory)

	// The conversation is loaded once: the thread shows it, and a fork's
	// next report is read from it (its summary's state is on its row).
	var msgs []store.MessageWithID
	if view != "map" || sess.ForkedAtMessageID != 0 {
		if msgs, err = u.store.LoadMessagesWithID(ctx, sessionID); err != nil {
			return nil, err
		}
	}
	if sess.ParentSessionID != "" {
		p.Parent = &chat.ParentInfo{MessageID: sess.ForkedAtMessageID}
		// Whether the viewer may see the parent, and how many members would
		// read a report (the report button names them), in one query.
		if members, ok, _ := u.store.SessionMembership(ctx, sess.ParentSessionID, me.ID); ok {
			if parent, _ := u.store.GetSession(ctx, sess.ParentSessionID); parent != nil {
				p.Parent.Accessible, p.Parent.SessionID, p.Parent.Title = true, parent.SessionID, parent.Title
				p.Parent.Members = members
			}
		}
	}
	if sess.ForkedAtMessageID != 0 {
		state := u.sessions.ForkSummaryState(ctx, &sess)
		p.SummaryPending, p.SummaryFailed = state == session.SummaryPending, state == session.SummaryFailed
		report, err := u.sessions.ReportState(ctx, &sess, msgs, me)
		if err != nil {
			return nil, err
		}
		p.Report = &chat.ReportView{ReportState: report}
	}

	if view == "map" {
		m := chat.BuildMap(chat.Root(p.Node))
		p.Map = &m
		return p, nil
	}

	forks, err := u.store.ListForks(ctx, sessionID, me.ID)
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
	p.Thread = chat.BuildThread(msgs, me.ID, byMessage, u.sessions.PendingQuestions(ctx, sessionID), directory)
	if block == "page" || block == "thread" {
		files, err := u.store.ListSessionFiles(ctx, sessionID)
		if err != nil {
			return nil, err
		}
		// A task running shows its files apart, not under its turn.
		running, err := u.store.ListRunningTasks(ctx, sessionID, "")
		if err != nil {
			log.Printf("ui: running tasks of %s: %v", sessionID, err)
		}
		p.Thread = chat.AttachFiles(p.Thread, files, running, directory)
	}
	if p.Report != nil {
		p.Thread = chat.MarkReported(p.Thread, sess, p.Report.Refused == nil)
	}
	p.LastMessageID = chat.LastMessageID(p.Thread)
	// Who works: the participants at work, several at once, as the Agents
	// panel shows them. A fork's summary workflow is no participant.
	if !p.SummaryPending {
		p.Working = p.Participants.Working()
	}
	return p, nil
}

// agentDirectory names the agents of the installation as they are now, and
// the session's.
func (u *ui) agentDirectory(ctx context.Context, sess *store.Session) (chat.AgentDirectory, error) {
	agents, err := u.store.ListAgents(ctx)
	if err != nil {
		return chat.AgentDirectory{}, err
	}
	directory := chat.AgentDirectory{ByID: make(map[string]chat.AgentInfo, len(agents))}
	for _, a := range agents {
		info := chat.AgentInfo{ID: a.ID, Name: cmp.Or(a.Name, a.ID), Mention: a.MentionName(), Description: a.Description}
		directory.ByID[a.ID] = info
		directory.List = append(directory.List, info)
	}
	directory.Session = chat.AgentInfo{ID: sess.AgentID, Name: sess.AgentID, Mention: sess.AgentID}
	if a, ok := directory.ByID[sess.AgentID]; ok {
		directory.Session = a
	}
	return directory, nil
}

// agentsPanel is the Agents panel of a session, for me.
func (u *ui) agentsPanel(ctx context.Context, me *store.User, sess *store.Session, directory chat.AgentDirectory) chat.AgentsPanel {
	return chat.BuildAgents(u.sessions.Participants(ctx, sess.SessionID), *sess, me.ID, directory)
}

// renderAgents renders the Agents panel alone: its reload, and the answer
// to its buttons, with why a stop did not go (failed). It reads what the
// panel shows, not the whole page.
func (u *ui) renderAgents(w http.ResponseWriter, r *http.Request, failed string) {
	ctx := r.Context()
	sess, err := u.sessions.Get(ctx, chi.URLParam(r, "id"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	directory, err := u.agentDirectory(ctx, sess)
	if err != nil {
		log.Printf("ui: agents of %s: %v", sess.SessionID, err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	p := &chat.Page{Node: &chat.TreeNode{Session: *sess}}
	p.Participants = u.agentsPanel(ctx, auth.UserFrom(ctx), sess, directory)
	p.Participants.Error = failed
	chat.RenderFragment(w, "agents", p, r.Header.Get(chat.VersionHeader))
}

// renderPage renders a page or one of its fragments, or the error that kept
// it from being built.
func (u *ui) renderPage(w http.ResponseWriter, r *http.Request, sessionID, view, block string, adjust func(*chat.Page)) {
	p, err := u.buildPage(r.Context(), auth.UserFrom(r.Context()), sessionID, view, block)
	if errors.Is(err, session.ErrNotFound) {
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
	if block == "page" {
		chat.RenderPage(w, block, p)
		return
	}
	chat.RenderFragment(w, block, p, r.Header.Get(chat.VersionHeader))
}

// home opens the session most recently active, or the welcome page.
func (u *ui) home(w http.ResponseWriter, r *http.Request) {
	me := auth.UserFrom(r.Context())
	stats, err := u.store.ListSessionStats(r.Context(), me.ID)
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
	u.renderPage(w, r, "", "thread", "page", nil)
}

func (u *ui) sessionPage(w http.ResponseWriter, r *http.Request) {
	u.renderPage(w, r, chi.URLParam(r, "id"), "thread", "page", nil)
}

func (u *ui) mapPage(w http.ResponseWriter, r *http.Request) {
	u.renderPage(w, r, chi.URLParam(r, "id"), "map", "page", nil)
}

func (u *ui) threadFragment(w http.ResponseWriter, r *http.Request) {
	u.renderPage(w, r, chi.URLParam(r, "id"), "thread", "thread", nil)
}

// treeFragment refreshes the tree, which the user sees from any page.
func (u *ui) treeFragment(w http.ResponseWriter, r *http.Request) {
	current := r.URL.Query().Get("current")
	me := auth.UserFrom(r.Context())
	sessions, err := u.store.ListSessionsByUser(r.Context(), me.ID)
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	stats, err := u.store.ListSessionStats(r.Context(), me.ID)
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	chat.RenderFragment(w, "tree-items", &chat.Page{Roots: chat.BuildTree(sessions, stats, u.sessions.Statuses(r.Context()), current)}, r.Header.Get(chat.VersionHeader))
}

// pageTopics are what a page's stream carries: the session's events, for the
// thread and the rail, and the user's tree's (session.TreeTopic). One stream
// per page: a browser holds few connections to a server (six over HTTP/1.1),
// and every tab keeps its stream open.
func pageTopics(me *store.User, sessionID string) []string {
	if sessionID == "" {
		return []string{session.TreeTopic(me.ID)}
	}
	return []string{sessionID, session.TreeTopic(me.ID)}
}

// treeStream is the stream of a page with no session: the tree's alone.
func (u *ui) treeStream(w http.ResponseWriter, r *http.Request) {
	relaySSE(w, r, u.hub, sseKeepAlive, nil, pageTopics(auth.UserFrom(r.Context()), "")...)
}

// sessionStream is a session page's stream: its session's events and the
// user's tree's, while the user is a member. One who is not (any more) gets
// session.EventSessionGone, not a 404: EventSource would retry a 404 for
// ever, and the page would never learn (a page that slept through the
// session's deletion). Not a member and no such session look the same.
func (u *ui) sessionStream(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	me := auth.UserFrom(r.Context())
	ok, err := u.sessions.IsMember(r.Context(), sessionID, me.ID)
	if err != nil {
		log.Printf("membership check: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		writeGone(w)
		return
	}
	relaySSE(w, r, u.hub, sseKeepAlive, stillMember(r, u.sessions, sessionID), pageTopics(me, sessionID)...)
}

// --- Actions ---

func (u *ui) newSessionForm(w http.ResponseWriter, r *http.Request) {
	id, err := u.sessions.Open(r.Context(), auth.UserFrom(r.Context()), session.OpenOptions{})
	if err != nil {
		log.Printf("ui: new session: %v", err)
		http.Error(w, "Impossible de créer la session", http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/s/"+id)
}

// sendForm posts a member's message and answers with the refreshed thread.
func (u *ui) sendForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	text := r.FormValue("content")
	fail := func(msg string) {
		u.renderPage(w, r, sessionID, "thread", "thread", func(p *chat.Page) { p.Error = msg })
	}
	if strings.TrimSpace(text) == "" {
		fail("Le message est vide.")
		return
	}
	sess, err := u.sessions.Get(r.Context(), sessionID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	_, err = u.sessions.Deliver(r.Context(), sess, auth.UserFrom(r.Context()), text)
	switch {
	case errors.Is(err, session.ErrSummaryPending):
		fail("Le résumé de la session parente est en cours : patiente un instant.")
		return
	case errors.Is(err, session.ErrQueueFull):
		fail(queueFullText)
		return
	case err != nil:
		log.Printf("ui: send to %s: %v", sessionID, err)
		fail("Message non envoyé : " + err.Error())
		return
	}
	u.renderPage(w, r, sessionID, "thread", "thread", nil)
}

func (u *ui) forkForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	messageID, _ := strconv.ParseInt(r.FormValue("message_id"), 10, 64)
	f, err := u.sessions.Fork(r.Context(), sessionID, messageID, r.FormValue("purpose"), auth.UserFrom(r.Context()))
	if err != nil {
		log.Printf("ui: fork %s at %d: %v", sessionID, messageID, err)
		msg := "Fork impossible : " + err.Error()
		if errors.Is(err, session.ErrPurposeTooLong) {
			msg = "Fork impossible : le but tient en quelques phrases, la spec reste dans la session parente."
		}
		u.renderPage(w, r, sessionID, "thread", "thread", func(p *chat.Page) { p.Error = msg })
		return
	}
	goTo(w, r, "/s/"+f.SessionID)
}

// reportFragment is the fork's report section, reloaded on the fork's
// events.
func (u *ui) reportFragment(w http.ResponseWriter, r *http.Request) {
	u.renderPage(w, r, chi.URLParam(r, "id"), "thread", "report", nil)
}

// reportForm starts the fork's report to its parent, and answers with the
// report section: running, or why it did not start.
//
// A refusal the section already explains (its Reason, read again after the
// click) is not said twice; one it does not, because the state changed in
// between or the start failed, is.
func (u *ui) reportForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	var msg string
	switch err := u.sessions.ReportToParent(r.Context(), sessionID, auth.UserFrom(r.Context())); {
	case err == nil:
	case errors.Is(err, session.ErrNothingToReport):
		msg = "Rien de nouveau à rapporter."
	case errors.Is(err, session.ErrSummaryPending):
		msg = "Le brief du fork est en cours d'écriture : patiente un instant."
	case errors.Is(err, session.ErrNotParentMember), errors.Is(err, session.ErrNoParent), errors.Is(err, session.ErrNotAFork):
		msg = "Rapport impossible depuis cette session."
	default:
		log.Printf("ui: report %s: %v", sessionID, err)
		msg = "Le rapport n'a pas pu démarrer."
	}
	u.renderPage(w, r, sessionID, "thread", "report", func(p *chat.Page) {
		if p.Report == nil { // not a fork: the section says so rather than vanish
			p.Report = &chat.ReportView{ReportState: session.ReportState{Refused: session.ErrNotAFork}}
		}
		if p.Report.Reason() == "" {
			p.Report.Error = msg
		}
	})
}

func (u *ui) inviteForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	err := u.sessions.Invite(r.Context(), sessionID, strings.TrimSpace(r.FormValue("email")), auth.UserFrom(r.Context()).ID)
	if err != nil {
		msg := "Invitation impossible."
		if errors.Is(err, session.ErrNoSuchUser) {
			msg = "Aucun compte actif avec cet email."
		}
		u.renderPage(w, r, sessionID, "thread", "rail", func(p *chat.Page) { p.Error = msg })
		return
	}
	// The members show in the top bar and change the agent's default mode:
	// reload the page rather than the rail alone.
	goTo(w, r, "/s/"+sessionID)
}

func (u *ui) agentModeForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	switch err := u.sessions.SetAgentMode(r.Context(), sessionID, r.FormValue("mode")); {
	case errors.Is(err, session.ErrBadMode):
		http.Error(w, "Mode inconnu", http.StatusBadRequest)
		return
	case err != nil:
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/s/"+sessionID) // the composer's hint changes with the mode
}

func (u *ui) leaveForm(w http.ResponseWriter, r *http.Request) {
	if err := u.sessions.Leave(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context()).ID); err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/")
}

func (u *ui) deleteForm(w http.ResponseWriter, r *http.Request) {
	switch err := u.sessions.Delete(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context()).ID); {
	case errors.Is(err, session.ErrNotCreator):
		http.Error(w, "Seul le créateur de la session peut la supprimer.", http.StatusForbidden)
	case err != nil:
		http.Error(w, "Internal error", http.StatusInternalServerError)
	default:
		goTo(w, r, "/")
	}
}

// answerForm answers a question of the session in the URL, by any member.
func (u *ui) answerForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	workflowID := r.FormValue("workflow_id")
	switch err := u.sessions.Answer(r.Context(), sessionID, workflowID, strings.TrimSpace(r.FormValue("answer"))); {
	case errors.Is(err, session.ErrEmptyAnswer), errors.Is(err, session.ErrForeignQuestion):
		http.Error(w, "Réponse invalide", http.StatusBadRequest)
		return
	case err != nil:
		log.Printf("ui: answer %s: %v", workflowID, err)
		u.renderPage(w, r, sessionID, "thread", "thread", func(p *chat.Page) { p.Error = "Réponse non transmise." })
		return
	}
	u.renderPage(w, r, sessionID, "thread", "thread", nil)
}

// cancelForm stops the turns running that the user may stop: all of them
// for the session's creator, those answering their messages for another
// member (the button shows only when there is one).
func (u *ui) cancelForm(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	sess, err := u.sessions.Get(r.Context(), sessionID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch err := u.sessions.Cancel(r.Context(), sess, auth.UserFrom(r.Context())); {
	case err == nil, errors.Is(err, session.ErrNothingToStop):
	case errors.Is(err, session.ErrStopNotAllowed):
		u.renderPage(w, r, sessionID, "thread", "thread", func(p *chat.Page) { p.Error = stopRefusedText })
		return
	default:
		log.Printf("ui: cancel %s: %v", sessionID, err)
	}
	u.renderPage(w, r, sessionID, "thread", "thread", nil)
}

// stopRefusedText is why a member may not stop a turn.
const stopRefusedText = "Seul l'auteur du message en cours, ou le créateur de la session, peut arrêter ce tour."

// agentsFragment is the Agents panel, reloaded on the session's events.
func (u *ui) agentsFragment(w http.ResponseWriter, r *http.Request) {
	u.renderAgents(w, r, "")
}

// stopForm stops a participant's turn, the one its row showed (turn), and
// answers with the panel. The rights are checked again at the click, on the
// turn the participant runs now.
func (u *ui) stopForm(w http.ResponseWriter, r *http.Request) {
	sess, err := u.sessions.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	err = u.sessions.StopTurn(r.Context(), sess, chi.URLParam(r, "agent"), r.FormValue("turn"), auth.UserFrom(r.Context()))
	u.renderAgents(w, r, u.stopFailure(sess.SessionID, err))
}

// clearForm stops a participant's turn and drops its queue, by the
// session's creator, and answers with the panel.
func (u *ui) clearForm(w http.ResponseWriter, r *http.Request) {
	sess, err := u.sessions.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	err = u.sessions.Clear(r.Context(), sess, chi.URLParam(r, "agent"), auth.UserFrom(r.Context()))
	u.renderAgents(w, r, u.stopFailure(sess.SessionID, err))
}

// stopTaskForm stops a background task, the one its line showed (task), by
// who asked for it or the session's creator, and answers with the panel.
func (u *ui) stopTaskForm(w http.ResponseWriter, r *http.Request) {
	sess, err := u.sessions.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	err = u.sessions.StopTask(r.Context(), sess, r.FormValue("task"), auth.UserFrom(r.Context()))
	switch {
	case errors.Is(err, session.ErrStopNotAllowed):
		u.renderAgents(w, r, "Seul le membre qui a demandé une tâche, ou le créateur de la session, peut l'arrêter.")
	case err == nil, errors.Is(err, session.ErrTaskOver), errors.Is(err, session.ErrNoSuchTask):
		u.renderAgents(w, r, "")
	case errors.Is(err, session.ErrStopPending):
		u.renderAgents(w, r, "Arrêt enregistré : il sera transmis à la tâche sous peu.")
	default:
		log.Printf("ui: stop a task in %s: %v", sess.SessionID, err)
		u.renderAgents(w, r, "L'arrêt de la tâche n'a pas pu être envoyé.")
	}
}

// stopFailure is why a stop did not go, in words; "" when it went, or had
// nothing left to stop (the panel shows it).
func (u *ui) stopFailure(sessionID string, err error) string {
	switch {
	case err == nil, errors.Is(err, session.ErrNothingToStop), errors.Is(err, session.ErrTurnOver):
		return ""
	case errors.Is(err, session.ErrStopNotAllowed):
		return stopRefusedText
	case errors.Is(err, session.ErrClearNotAllowed):
		return "Seul le créateur de la session peut vider la file d'un agent."
	}
	log.Printf("ui: stop in %s: %v", sessionID, err)
	return "L'arrêt n'a pas pu être envoyé."
}

// --- Notifications ---

func (u *ui) notificationsPage(w http.ResponseWriter, r *http.Request) {
	me := auth.UserFrom(r.Context())
	msgs, err := u.store.LoadMessagesWithID(r.Context(), notificationsOf(me.ID))
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

func (u *ui) deleteNotificationForm(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "notifID"), 10, 64)
	if err == nil {
		err = u.store.DeleteMessage(r.Context(), notificationsOf(auth.UserFrom(r.Context()).ID), id)
	}
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	goTo(w, r, "/notifications")
}
