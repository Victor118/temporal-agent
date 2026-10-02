package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/victor/temporal-agent/store"
)

const (
	// CookieName holds the login token, for the chat and the back-office alike.
	CookieName = "session_token"
	// SessionTTL is how long a login lasts.
	SessionTTL = 30 * 24 * time.Hour
)

// LoginFailDelay slows down password guessing: every failed login costs it.
var LoginFailDelay = time.Second

// ErrInvalidCredentials is the one answer to a failed login: it does not say
// whether the email exists.
var ErrInvalidCredentials = errors.New("email ou mot de passe incorrect")

// ErrTooManyAttempts refuses a login after too many failures, from the same
// address or on the same account. It is answered before the password is
// checked, so it says nothing about the password either.
var ErrTooManyAttempts = errors.New("trop de tentatives, réessaie plus tard")

// dummyHash is checked against when the email is unknown, so a login takes
// the same time whether the account exists or not.
var dummyHash, _ = HashPassword("not a real password, only spends time")

// UserStore is what authentication needs of the store: accounts by email,
// and login sessions.
type UserStore interface {
	GetUserByEmail(ctx context.Context, email string) (*store.User, error)
	CreateLoginSession(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error
	GetLoginSessionUser(ctx context.Context, tokenHash string) (*store.User, error)
	DeleteLoginSession(ctx context.Context, tokenHash string) error
}

// Service logs users in and out, and finds the user behind a request.
type Service struct {
	Store UserStore
	// Limits bound failed logins; nil = no limit.
	Limits *LoginLimits
	// Clients tells the address a login comes from.
	Clients ClientAddrs
}

// ClientAddr is the address r comes from, to pass to Login and Authenticate;
// "" when it is not known.
func (s *Service) ClientAddr(r *http.Request) string { return s.Clients.Of(r) }

// Login checks an email and password and opens a login session. It returns
// the token to hand to the browser, never stored as is. client is the address
// the attempt comes from (s.ClientAddr).
func (s *Service) Login(ctx context.Context, client, email, password string) (string, *store.User, error) {
	u, err := s.Authenticate(ctx, client, email, password)
	if err != nil {
		return "", nil, err
	}
	token, err := s.StartSession(ctx, u)
	return token, u, err
}

// Authenticate checks an email and password without opening a session, for a
// caller that has more to check first (the back-office: is it an admin?).
//
// Failures are counted per client and per account, and logged with the
// client's address. The delay on a failure only slows a sequential guesser;
// the limits stop parallel ones.
func (s *Service) Authenticate(ctx context.Context, client, email, password string) (*store.User, error) {
	if s.Limits.Blocked(client, email) {
		log.Printf("auth: login refused for %q from %s: too many failures", email, describeClient(client))
		return nil, ErrTooManyAttempts
	}
	u, err := s.Store.GetUserByEmail(ctx, email)
	if err != nil {
		return nil, err
	}
	hash := dummyHash
	if u != nil {
		hash = u.PasswordHash
	}
	ok, err := VerifyPassword(password, hash)
	if err != nil && u != nil {
		log.Printf("auth: user %s has an unreadable password hash: %v", u.ID, err)
	}
	if u == nil || !ok || u.DisabledAt != nil {
		log.Printf("auth: failed login for %q from %s", email, describeClient(client))
		s.Limits.Fail(client, email)
		time.Sleep(LoginFailDelay)
		return nil, ErrInvalidCredentials
	}
	s.Limits.Succeed(email)
	return u, nil
}

// describeClient is client for the logs, an unknown one included.
func describeClient(client string) string {
	if client == "" {
		return "an unknown address"
	}
	return client
}

// StartSession opens a login session for u and returns its token.
func (s *Service) StartSession(ctx context.Context, u *store.User) (string, error) {
	token := newToken()
	if err := s.Store.CreateLoginSession(ctx, hashToken(token), u.ID, time.Now().Add(SessionTTL)); err != nil {
		return "", err
	}
	return token, nil
}

// Logout ends the login session of the request, if any.
func (s *Service) Logout(ctx context.Context, r *http.Request) {
	if c, err := r.Cookie(CookieName); err == nil {
		if err := s.Store.DeleteLoginSession(ctx, hashToken(c.Value)); err != nil {
			log.Printf("auth: logout: %v", err)
		}
	}
}

// UserFromRequest returns the logged-in user of a request, or nil.
func (s *Service) UserFromRequest(r *http.Request) (*store.User, error) {
	c, err := r.Cookie(CookieName)
	if err != nil || c.Value == "" {
		return nil, nil
	}
	return s.Store.GetLoginSessionUser(r.Context(), hashToken(c.Value))
}

// SetCookie hands the login token to the browser: HttpOnly so page scripts
// cannot read it, SameSite=Strict so other sites cannot send it.
func SetCookie(w http.ResponseWriter, r *http.Request, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https",
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(SessionTTL.Seconds()),
	})
}

func ClearCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: CookieName, Value: "", Path: "/", MaxAge: -1, HttpOnly: true})
}

type ctxKey struct{}

// WithUser stores the logged-in user in ctx.
func WithUser(ctx context.Context, u *store.User) context.Context {
	return context.WithValue(ctx, ctxKey{}, u)
}

// UserFrom returns the logged-in user stored by the middleware, or nil.
func UserFrom(ctx context.Context) *store.User {
	u, _ := ctx.Value(ctxKey{}).(*store.User)
	return u
}

// RequireUser answers 401 to a request without a logged-in user, and puts
// the user in the request context otherwise.
func (s *Service) RequireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, err := s.UserFromRequest(r)
		if err != nil {
			log.Printf("auth: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if u == nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), u)))
	})
}

// RequireAdmin refuses a user who is not an admin. It runs after RequireUser.
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u := UserFrom(r.Context()); u == nil || !u.IsAdmin() {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// SameOrigin refuses state-changing requests sent from another site. The
// cookie is SameSite=Strict already; this does not rely on the browser alone.
func SameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			origin, err := url.Parse(r.Header.Get("Origin"))
			if err != nil || origin.Host != r.Host {
				http.Error(w, "Origin refused", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func newToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return hex.EncodeToString(b)
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
