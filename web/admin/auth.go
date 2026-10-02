package admin

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/victor/temporal-agent/auth"
)

// requireLogin lets through admins only. Anyone else is sent to the login
// page, which says why when the account is not an admin.
func (a *Admin) requireLogin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, err := a.cfg.Auth.UserFromRequest(r)
		if err != nil {
			log.Printf("admin: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if u != nil && u.IsAdmin() {
			next.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), u)))
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

type loginData struct {
	Next  string
	Email string
	Error string
}

func (a *Admin) loginPage(w http.ResponseWriter, r *http.Request) {
	data := loginData{Next: r.URL.Query().Get("next")}
	u, err := a.cfg.Auth.UserFromRequest(r)
	if err == nil && u != nil {
		if u.IsAdmin() {
			http.Redirect(w, r, safeNext(data.Next), http.StatusSeeOther)
			return
		}
		data.Email = u.Email
		data.Error = "Ce compte n'est pas administrateur. Connecte-toi avec un compte admin."
	}
	a.pages.execute(w, "login", "login", pageData{Data: data})
}

func (a *Admin) login(w http.ResponseWriter, r *http.Request) {
	data := loginData{Next: r.FormValue("next"), Email: r.FormValue("email")}
	u, err := a.cfg.Auth.Authenticate(r.Context(), a.cfg.Auth.ClientAddr(r), data.Email, r.FormValue("password"))
	switch {
	case errors.Is(err, auth.ErrInvalidCredentials):
		data.Error = "Email ou mot de passe incorrect."
	case errors.Is(err, auth.ErrTooManyAttempts):
		data.Error = "Trop de tentatives : réessaie dans quelques minutes."
		w.WriteHeader(http.StatusTooManyRequests)
	case err != nil:
		log.Printf("admin: login: %v", err)
		data.Error = "Erreur interne."
	case !u.IsAdmin():
		// Checked before opening a session: a non-admin gets no session
		// from the back-office.
		data.Error = "Ce compte n'est pas administrateur."
	default:
		token, err := a.cfg.Auth.StartSession(r.Context(), u)
		if err != nil {
			log.Printf("admin: login: %v", err)
			data.Error = "Erreur interne."
			break
		}
		auth.SetCookie(w, r, token)
		http.Redirect(w, r, safeNext(data.Next), http.StatusSeeOther)
		return
	}
	a.pages.execute(w, "login", "login", pageData{Data: data})
}

// logout ends the login session: the chat's too, it is the same one.
func (a *Admin) logout(w http.ResponseWriter, r *http.Request) {
	a.cfg.Auth.Logout(r.Context(), r)
	auth.ClearCookie(w)
	navigate(w, r, "/admin/login")
}

// safeNext keeps the post-login redirect inside the back-office, so the login
// page cannot be used to bounce someone to another site.
func safeNext(next string) string {
	if strings.HasPrefix(next, "/admin") && !strings.HasPrefix(next, "//") && !strings.Contains(next, `\`) {
		return next
	}
	return "/admin/"
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
