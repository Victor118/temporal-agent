package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/web/admin"
)

// webhookStore answers the lookups the webhooks make.
type webhookStore struct {
	routeStore
	telegramLookups int
	skillsVersion   int64
}

func (f *webhookStore) GetUserByTelegramID(context.Context, int64) (*store.User, error) {
	f.telegramLookups++
	return nil, nil
}

func (f *webhookStore) IncrementSkillsVersion(context.Context) (int64, error) {
	f.skillsVersion++
	return f.skillsVersion, nil
}

func newWebhookTest(t *testing.T, cfg *config.Config) (http.Handler, *webhookStore) {
	t.Helper()
	st := &webhookStore{routeStore: routeStore{logins: map[string]string{}}}
	svc := &auth.Service{Store: st}
	h := &handler{auth: svc, store: st, hub: sse.NewHub(), cfg: cfg}
	return publicRouter(h, admin.New(admin.Config{Store: st, Auth: svc})), st
}

func post(h http.Handler, path, body string, headers map[string]string) int {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code
}

const telegramUpdate = `{"update_id":1,"message":{"message_id":1,"chat":{"id":42},"text":"approve"}}`

// Without a secret configured, nobody can speak for a Telegram user: the route
// does not exist.
func TestTelegramWebhook_ClosedWithoutSecret(t *testing.T) {
	h, st := newWebhookTest(t, &config.Config{TelegramBotToken: "token"})
	if code := post(h, "/webhooks/telegram", telegramUpdate, nil); code != http.StatusNotFound {
		t.Errorf("no secret configured: %d, want 404", code)
	}
	if st.telegramLookups != 0 {
		t.Error("an unauthenticated update reached the user lookup")
	}
}

func TestTelegramWebhook_RequiresTheSecret(t *testing.T) {
	h, st := newWebhookTest(t, &config.Config{TelegramWebhookSecret: "s3cret"})
	for name, headers := range map[string]map[string]string{
		"no header":    nil,
		"wrong secret": {telegramSecretHeader: "guess"},
		"empty secret": {telegramSecretHeader: ""},
	} {
		if code := post(h, "/webhooks/telegram", telegramUpdate, headers); code != http.StatusUnauthorized {
			t.Errorf("%s: %d, want 401", name, code)
		}
	}
	if st.telegramLookups != 0 {
		t.Fatal("an unauthenticated update reached the user lookup")
	}
	if code := post(h, "/webhooks/telegram", telegramUpdate, map[string]string{telegramSecretHeader: "s3cret"}); code != http.StatusOK {
		t.Errorf("right secret: %d, want 200", code)
	}
	if st.telegramLookups != 1 {
		t.Errorf("the authenticated update was not processed")
	}
}

func signGitHub(body, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Every skills webhook makes the server and the workers clone the repo again:
// unsigned, anyone could keep them busy.
func TestSkillsWebhook_ClosedWithoutSecret(t *testing.T) {
	h, st := newWebhookTest(t, &config.Config{SkillsRepo: "https://example.com/skills"})
	if code := post(h, "/webhooks/skills", `{}`, nil); code != http.StatusNotFound {
		t.Errorf("no secret configured: %d, want 404", code)
	}
	if st.skillsVersion != 0 {
		t.Error("the skills version moved")
	}
}

func TestSkillsWebhook_RequiresTheSignature(t *testing.T) {
	h, st := newWebhookTest(t, &config.Config{SkillsWebhookSecret: "s3cret"})
	body := `{"ref":"refs/heads/main"}`
	if code := post(h, "/webhooks/skills", body, map[string]string{"X-Hub-Signature-256": signGitHub(body, "other")}); code != http.StatusUnauthorized {
		t.Errorf("bad signature: %d, want 401", code)
	}
	if code := post(h, "/webhooks/skills", body, nil); code != http.StatusUnauthorized {
		t.Errorf("no signature: %d, want 401", code)
	}
	if code := post(h, "/webhooks/skills", body, map[string]string{"X-Hub-Signature-256": signGitHub(body, "s3cret")}); code != http.StatusNoContent {
		t.Errorf("good signature: %d, want 204", code)
	}
	if st.skillsVersion != 1 {
		t.Errorf("skills version %d, want 1", st.skillsVersion)
	}
}

func TestVerifyGitHubSignature_EmptySecretMatchesNothing(t *testing.T) {
	if verifyGitHubSignature([]byte("x"), signGitHub("x", ""), "") {
		t.Error("an empty secret accepted a signature")
	}
}

// The internal endpoint can put any event in any session: only a worker
// holding the key may use it, and none may when no key is configured.
func TestInternalNotify_RequiresTheKey(t *testing.T) {
	event := `{"session_id":"s1","event":{"type":"message","data":{"content":"hi"}}}`
	for _, c := range []struct {
		name       string
		key, given string
		want       int
	}{
		{"no key configured", "", "Bearer ", http.StatusUnauthorized},
		{"no header", "k3y", "", http.StatusUnauthorized},
		{"wrong key", "k3y", "Bearer nope", http.StatusUnauthorized},
		{"not bearer", "k3y", "k3y", http.StatusUnauthorized},
		{"right key", "k3y", "Bearer k3y", http.StatusNoContent},
	} {
		t.Run(c.name, func(t *testing.T) {
			hub := sse.NewHub()
			ch := hub.Subscribe("s1")
			defer hub.Unsubscribe("s1", ch)
			code := post(handleInternalNotify(hub, c.key), "/internal/notify", event, map[string]string{"Authorization": c.given})
			if code != c.want {
				t.Fatalf("%d, want %d", code, c.want)
			}
			select {
			case <-ch:
				if c.want != http.StatusNoContent {
					t.Error("a refused notification reached the session")
				}
			case <-time.After(50 * time.Millisecond):
				if c.want == http.StatusNoContent {
					t.Error("an accepted notification never reached the session")
				}
			}
		})
	}
}
