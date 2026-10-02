package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/web"
	"github.com/victor/temporal-agent/web/admin"
	"github.com/victor/temporal-agent/web/chat"
)

// publicRouter is the public HTTP API, shared by `agent server` and `agent dev`.
func publicRouter(h *handler, adminUI *admin.Admin) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Unauthenticated routes. Webhooks authenticate themselves, with a secret
	// of their own: without it configured, the route does not exist.
	r.Handle("/static/*", http.StripPrefix("/static/", chat.Static()))
	if h.cfg.SkillsWebhookSecret != "" {
		r.Post("/webhooks/skills", h.handleSkillsWebhook)
	}
	if h.cfg.TelegramWebhookSecret != "" {
		r.Post("/webhooks/telegram", h.handleTelegramWebhook)
	}

	// Back-office: its own login page, open to admins only
	r.Mount("/admin", adminUI.Routes())

	// The interface: pages, and the fragments htmx swaps in
	r.Group(func(r chi.Router) {
		r.Use(auth.SameOrigin)
		r.Get("/login", h.loginPage)
		r.Post("/login", h.loginForm)
		r.Post("/logout", h.logoutForm)

		r.Group(func(r chi.Router) {
			r.Use(h.requireUserPage)
			r.Get("/", h.home)
			r.Get("/tree", h.treeFragment)
			r.Post("/s/new", h.newSessionForm)
			r.Get("/notifications", h.notificationsPage)
			r.Post("/notifications/{notifID}/delete", h.deleteNotificationForm)
			// The former single-page chat, kept while the interface settles.
			r.Get("/classic", web.HandleIndex)

			r.Route("/s/{id}", func(r chi.Router) {
				r.Use(h.requireMember)
				r.Get("/", h.sessionPage)
				r.Get("/map", h.mapPage)
				r.Get("/thread", h.threadFragment)
				r.Post("/messages", h.sendForm)
				r.Post("/fork", h.forkForm)
				r.Post("/members", h.inviteForm)
				r.Post("/agent-mode", h.agentModeForm)
				r.Post("/leave", h.leaveForm)
				r.Post("/delete", h.deleteForm)
				r.Post("/answer", h.answerForm)
				r.Post("/cancel", h.cancelForm)
			})
		})
	})

	r.Group(func(r chi.Router) {
		r.Use(auth.SameOrigin)
		r.Post("/auth/login", h.login)
		r.Post("/auth/logout", h.logout)

		// Logged-in user
		r.Group(func(r chi.Router) {
			r.Use(h.auth.RequireUser)

			r.Get("/auth/me", h.me)
			r.Get("/me/sessions", h.listSessions)
			r.Get("/me/notifications", h.getNotifications)
			r.Get("/me/notifications/stream", h.streamNotifications)
			r.Delete("/me/notifications/{notifID}", h.deleteNotification)
			r.Delete("/me/notifications", h.deleteAllNotifications)
			r.Post("/sessions", h.createSession)

			// Members of the session only
			r.Route("/sessions/{id}", func(r chi.Router) {
				r.Use(h.requireMember)
				r.Get("/", h.getSessionInfo)
				r.Post("/fork", h.forkSession)
				r.Post("/messages", h.sendMessage)
				r.Put("/agent-mode", h.setAgentMode)
				r.Post("/cancel", h.cancelAgent)
				r.Delete("/", h.deleteSession)
				r.Get("/state", h.getState)
				r.Get("/history", h.getHistory)
				r.Get("/stream", h.stream)
				r.Post("/answer", h.answerQuestion)
				r.Get("/members", h.listMembers)
				r.Post("/members", h.addMember)
				r.Delete("/members/{userID}", h.removeMember)
			})

			// Admin JSON API (the chat's activity queue panel)
			r.Group(func(r chi.Router) {
				r.Use(auth.RequireAdmin)
				r.Get("/api/admin/queues", h.listKnownQueues)
				r.Get("/api/admin/activity-queues", h.listActivityQueues)
				r.Put("/api/admin/activity-queues", h.setActivityQueue)
				r.Delete("/api/admin/activity-queues/{activityName}", h.deleteActivityQueue)
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
