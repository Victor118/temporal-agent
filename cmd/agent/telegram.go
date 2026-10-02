package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/telegram"
)

// telegramSecretHeader carries the secret_token given to setWebhook. It is the
// only proof that an update comes from Telegram: the chat ID in the body is
// guessable, and it decides whose agent runs and whose questions get answered.
const telegramSecretHeader = "X-Telegram-Bot-Api-Secret-Token"

// telegramUsers finds the account a Telegram chat belongs to.
type telegramUsers interface {
	GetUserByTelegramID(ctx context.Context, telegramID int64) (*store.User, error)
}

// telegramChannel takes Telegram updates into sessions: a user's chat is
// their session on this channel, a message is delivered like one typed on
// the web, and a message while a question waits is its answer.
type telegramChannel struct {
	sessions *session.Service
	users    telegramUsers
	secret   string // TELEGRAM_WEBHOOK_SECRET; never empty once the route is mounted
}

func (t *telegramChannel) Name() string { return telegram.Channel }

func (t *telegramChannel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !secretEqual(r.Header.Get(telegramSecretHeader), t.secret) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	var update telegram.Update
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxWebhookBytes)).Decode(&update); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// A photo, sticker or voice note has no text. Acknowledge it so Telegram
	// stops retrying, but never turn it into an empty user message.
	if update.Message == nil || strings.TrimSpace(update.Message.Text) == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	chatID := update.Message.Chat.ID
	text := update.Message.Text

	user, err := t.users.GetUserByTelegramID(r.Context(), chatID)
	if err != nil {
		log.Printf("Telegram webhook: error looking up user: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if user == nil || user.DisabledAt != nil {
		log.Printf("Telegram webhook: unknown or disabled telegram_id %d", chatID)
		w.WriteHeader(http.StatusOK) // Return 200 to Telegram so it doesn't retry
		return
	}
	channelID := strconv.FormatInt(chatID, 10)

	// /new opens a fresh session, which the chat's next messages go to.
	if text == "/new" {
		id, err := t.sessions.Open(r.Context(), user, session.OpenOptions{Channel: telegram.Channel, ChannelID: channelID})
		if err != nil {
			log.Printf("Telegram webhook: failed to create session: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		log.Printf("Telegram: new session %s for user %s", id, user.ID)
		w.WriteHeader(http.StatusOK)
		return
	}

	sess, err := t.sessions.OpenOnChannel(r.Context(), user, telegram.Channel, channelID)
	if err != nil {
		log.Printf("Telegram webhook: find or open session: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	// Telegram has no answer button: while a question waits, the next
	// message answers it.
	if t.sessions.AnswerPending(r.Context(), sess.SessionID, text) {
		w.WriteHeader(http.StatusOK)
		return
	}
	if _, err := t.sessions.Deliver(r.Context(), sess, user, text); err != nil {
		log.Printf("Telegram webhook: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
