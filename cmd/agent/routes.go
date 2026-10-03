package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/web/chat"
)

// serverStore is everything the HTTP side reads and writes, adapter by
// adapter. *store.PostgresStore is one.
type serverStore interface {
	session.Store
	readStore
	telegramUsers
	skillsVersionBumper
	activityQueueStore
}

// inboundChannel takes messages from a channel's webhook into sessions. A new
// channel is one more implementation, mounted at /webhooks/<name>; on the way
// out, the same name selects its activity.Notifier.
type inboundChannel interface {
	http.Handler
	Name() string
}

// server holds the HTTP adapters. Each one is thin: it decodes, calls the
// session service or reads the store, and answers in its own format.
type server struct {
	auth     *auth.Service
	sessions *session.Service
	api      *api
	ui       *ui
	queues   *activityQueuesAPI
	// channels take messages in from outside, each on its own webhook.
	// Mounted only with their secret configured.
	channels []inboundChannel
	skills   *skillsWebhook // nil = no secret, no route
	admin    http.Handler   // the back-office, mounted under /admin
}

// newServer wires the adapters over one session service.
func newServer(cfg *config.Config, st serverStore, tc session.Temporal, hub *sse.Hub, authSvc *auth.Service, adminUI http.Handler) *server {
	sessions := session.New(st, tc, hub, session.Config{
		Namespace:      cfg.TemporalNamespace,
		WorkflowQueue:  cfg.WorkflowQueue,
		DefaultAgentID: cfg.DefaultAgentID,
		SummaryModel:   cfg.SummaryModel,
	})
	// Every event the hub publishes, from the workers or from here, goes
	// through the service first: the turn events feed what it shows.
	hub.Observe(sessions.Observe)
	s := &server{
		auth:     authSvc,
		sessions: sessions,
		api:      &api{auth: authSvc, sessions: sessions, store: st, hub: hub},
		ui:       &ui{auth: authSvc, sessions: sessions, store: st, hub: hub},
		queues:   &activityQueuesAPI{store: st, workflowQueue: cfg.WorkflowQueue},
		admin:    adminUI,
	}
	if cfg.TelegramWebhookSecret != "" {
		s.channels = append(s.channels, &telegramChannel{sessions: sessions, users: st, secret: cfg.TelegramWebhookSecret})
	}
	if cfg.SkillsWebhookSecret != "" {
		s.skills = &skillsWebhook{secret: cfg.SkillsWebhookSecret, store: st}
	}
	return s
}

// routes is the public HTTP API, shared by `agent server` and `agent dev`.
func (s *server) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Unauthenticated routes. Webhooks authenticate themselves, with a secret
	// of their own: without it configured, the route does not exist.
	r.Handle("/static/*", http.StripPrefix("/static/", chat.Static()))
	if s.skills != nil {
		r.Post("/webhooks/skills", s.skills.ServeHTTP)
	}
	for _, c := range s.channels {
		r.Post("/webhooks/"+c.Name(), c.ServeHTTP)
	}

	// Back-office: its own login page, open to admins only
	r.Mount("/admin", s.admin)

	member := requireMember(s.sessions)

	// The interface: pages, and the fragments htmx swaps in
	u := s.ui
	r.Group(func(r chi.Router) {
		r.Use(auth.SameOrigin)
		r.Get("/login", u.loginPage)
		r.Post("/login", u.loginForm)
		r.Post("/logout", u.logoutForm)

		r.Group(func(r chi.Router) {
			r.Use(u.requireUserPage)
			r.Get("/", u.home)
			r.Get("/tree", u.treeFragment)
			r.Get("/tree/stream", u.treeStream)
			r.Post("/s/new", u.newSessionForm)
			r.Get("/notifications", u.notificationsPage)
			r.Post("/notifications/{notifID}/delete", u.deleteNotificationForm)

			r.Route("/s/{id}", func(r chi.Router) {
				r.Use(member)
				r.Get("/", u.sessionPage)
				r.Get("/map", u.mapPage)
				r.Get("/thread", u.threadFragment)
				r.Post("/messages", u.sendForm)
				r.Post("/fork", u.forkForm)
				r.Get("/report", u.reportFragment)
				r.Post("/report", u.reportForm)
				r.Post("/members", u.inviteForm)
				r.Post("/agent-mode", u.agentModeForm)
				r.Post("/leave", u.leaveForm)
				r.Post("/delete", u.deleteForm)
				r.Post("/answer", u.answerForm)
				r.Post("/cancel", u.cancelForm)
			})
		})
	})

	a := s.api
	r.Group(func(r chi.Router) {
		r.Use(auth.SameOrigin)
		r.Post("/auth/login", a.login)
		r.Post("/auth/logout", a.logout)

		// Logged-in user
		r.Group(func(r chi.Router) {
			r.Use(s.auth.RequireUser)

			r.Get("/auth/me", a.me)
			r.Get("/me/sessions", a.listSessions)
			r.Get("/me/notifications", a.getNotifications)
			r.Get("/me/notifications/stream", a.streamNotifications)
			r.Delete("/me/notifications/{notifID}", a.deleteNotification)
			r.Delete("/me/notifications", a.deleteAllNotifications)
			r.Post("/sessions", a.createSession)

			// Members of the session only
			r.Route("/sessions/{id}", func(r chi.Router) {
				r.Use(member)
				r.Get("/", a.getSessionInfo)
				r.Post("/fork", a.forkSession)
				r.Post("/report", a.reportToParent)
				r.Post("/messages", a.sendMessage)
				r.Put("/agent-mode", a.setAgentMode)
				r.Post("/cancel", a.cancelAgent)
				r.Delete("/", a.deleteSession)
				r.Get("/state", a.getState)
				r.Get("/history", a.getHistory)
				r.Get("/stream", a.stream)
				r.Post("/answer", a.answerQuestion)
				r.Get("/members", a.listMembers)
				r.Post("/members", a.addMember)
				r.Delete("/members/{userID}", a.removeMember)
			})

			// Admin JSON API (the chat's activity queue panel)
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireAdmin)
				r.Get("/api/admin/queues", s.queues.listKnownQueues)
				r.Get("/api/admin/activity-queues", s.queues.listActivityQueues)
				r.Put("/api/admin/activity-queues", s.queues.setActivityQueue)
				r.Delete("/api/admin/activity-queues/{activityName}", s.queues.deleteActivityQueue)
			})
		})
	})
	return r
}

// warnClosedWebhooks says at startup which configured integration has no
// webhook because its secret is missing: the route is silently absent
// otherwise, and the integration just never hears anything.
func warnClosedWebhooks(cfg *config.Config) {
	if cfg.SkillsRepo != "" && cfg.SkillsWebhookSecret == "" {
		log.Println("Warning: SKILLS_WEBHOOK_SECRET is not set, /webhooks/skills is disabled (skills reload from the back-office only)")
	}
	if cfg.TelegramBotToken != "" && cfg.TelegramWebhookSecret == "" {
		log.Println("Warning: TELEGRAM_WEBHOOK_SECRET is not set, /webhooks/telegram is disabled")
	}
}
