package admin

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	sessionCookie = "admin_session"
	sessionTTL    = 7 * 24 * time.Hour
)

// loginFailDelay slows down guessing the single shared password.
var loginFailDelay = time.Second

// sessionStore holds admin session tokens in memory: a restart logs everyone
// out. Real user accounts, with sessions in the DB, will replace it.
type sessionStore struct {
	mu     sync.Mutex
	tokens map[string]time.Time // token → expiry
}

func newSessionStore() *sessionStore {
	return &sessionStore{tokens: make(map[string]time.Time)}
}

func (s *sessionStore) create() string {
	b := make([]byte, 32)
	rand.Read(b)
	token := hex.EncodeToString(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[token] = time.Now().Add(sessionTTL)
	return token
}

func (s *sessionStore) valid(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	expiry, ok := s.tokens[token]
	if ok && time.Now().After(expiry) {
		delete(s.tokens, token)
		return false
	}
	return ok
}

func (s *sessionStore) revoke(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tokens, token)
}

func (a *Admin) loggedIn(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	return err == nil && a.sessions.valid(c.Value)
}

// requireLogin sends anyone without an admin session to the login page. With
// no ADMIN_API_KEY nobody can log in, so the back-office stays closed.
func (a *Admin) requireLogin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.loggedIn(r) {
			next.ServeHTTP(w, r)
			return
		}
		login := "/admin/login?next=" + url.QueryEscape(r.URL.RequestURI())
		if r.Header.Get("HX-Request") != "" {
			// A redirect would be followed by the XHR and swapped in place.
			w.Header().Set("HX-Redirect", login)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, login, http.StatusSeeOther)
	})
}

// sameOrigin refuses state-changing requests sent from another site. The
// cookie is SameSite=Strict already; this does not rely on the browser alone.
func sameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			origin, err := url.Parse(r.Header.Get("Origin"))
			if err != nil || origin.Host != r.Host {
				http.Error(w, "Origine refusée", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

type loginData struct {
	Enabled bool
	Next    string
	Error   string
}

func (a *Admin) loginPage(w http.ResponseWriter, r *http.Request) {
	if a.loggedIn(r) {
		http.Redirect(w, r, safeNext(r.URL.Query().Get("next")), http.StatusSeeOther)
		return
	}
	a.pages.execute(w, "login", "login", pageData{Data: loginData{
		Enabled: a.cfg.AdminKey != "",
		Next:    r.URL.Query().Get("next"),
	}})
}

func (a *Admin) login(w http.ResponseWriter, r *http.Request) {
	next := r.FormValue("next")
	if a.cfg.AdminKey == "" || !passwordMatches(r.FormValue("password"), a.cfg.AdminKey) {
		time.Sleep(loginFailDelay)
		a.pages.execute(w, "login", "login", pageData{Data: loginData{
			Enabled: a.cfg.AdminKey != "",
			Next:    next,
			Error:   "Mot de passe incorrect.",
		}})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    a.sessions.create(),
		Path:     "/admin",
		HttpOnly: true,
		Secure:   isHTTPS(r),
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(sessionTTL.Seconds()),
	})
	http.Redirect(w, r, safeNext(next), http.StatusSeeOther)
}

func (a *Admin) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		a.sessions.revoke(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/admin", MaxAge: -1})
	navigate(w, r, "/admin/login")
}

// passwordMatches compares digests, so the comparison takes the same time
// whatever the length of the guess.
func passwordMatches(guess, key string) bool {
	g, k := sha256.Sum256([]byte(guess)), sha256.Sum256([]byte(key))
	return subtle.ConstantTimeCompare(g[:], k[:]) == 1
}

// safeNext keeps the post-login redirect inside the back-office, so the login
// page cannot be used to bounce someone to another site.
func safeNext(next string) string {
	if strings.HasPrefix(next, "/admin") && !strings.HasPrefix(next, "//") && !strings.Contains(next, `\`) {
		return next
	}
	return "/admin/"
}

func isHTTPS(r *http.Request) bool {
	return r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
}

// navigate sends the browser to url after a form post: through htmx when the
// form was boosted, through a plain redirect otherwise.
func navigate(w http.ResponseWriter, r *http.Request, url string) {
	if r.Header.Get("HX-Request") != "" {
		w.Header().Set("HX-Location", url)
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, url, http.StatusSeeOther)
}
