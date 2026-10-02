package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/web"
	"github.com/victor/temporal-agent/web/admin"
)

// publicRouter is the public HTTP API, shared by `agent server` and `agent dev`.
func publicRouter(h *handler, adminUI *admin.Admin) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Unauthenticated routes. Webhooks authenticate themselves.
	r.Get("/", web.HandleIndex)
	r.Post("/webhooks/skills", h.handleSkillsWebhook)
	r.Post("/webhooks/telegram", h.handleTelegramWebhook)

	// Back-office: its own login page, open to admins only
	r.Mount("/admin", adminUI.Routes())

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
