package admin

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/store"
)

// userForm is the create/edit form of a user, holding what was typed so a
// refused submission comes back intact.
type userForm struct {
	New         bool
	ID          string
	Email       string
	DisplayName string
	Role        string
	TelegramID  string
	Password    string // create only: the initial password
	Disabled    bool
	IsSelf      bool // the admin editing their own account
	Error       string
}

func (a *Admin) users(w http.ResponseWriter, r *http.Request) {
	users, err := a.cfg.Store.ListUsers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.page(w, r, "users", "users", func(*Inventory) (any, bool) { return users, true })
}

func (a *Admin) renderUserForm(w http.ResponseWriter, r *http.Request, f userForm) {
	f.IsSelf = !f.New && f.ID == auth.UserFrom(r.Context()).ID
	a.page(w, r, "user_edit", "users", func(*Inventory) (any, bool) { return f, true })
}

func (a *Admin) newUserForm(w http.ResponseWriter, r *http.Request) {
	a.renderUserForm(w, r, userForm{New: true, Role: store.UserRoleStandard})
}

func (a *Admin) editUserForm(w http.ResponseWriter, r *http.Request) {
	u, err := a.cfg.Store.GetUser(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if u == nil {
		http.NotFound(w, r)
		return
	}
	f := userForm{ID: u.ID, Email: u.Email, DisplayName: u.DisplayName, Role: u.Role, Disabled: u.DisabledAt != nil}
	if u.TelegramID != nil {
		f.TelegramID = strconv.FormatInt(*u.TelegramID, 10)
	}
	a.renderUserForm(w, r, f)
}

func userFormFromRequest(r *http.Request) userForm {
	return userForm{
		Email:       strings.TrimSpace(r.FormValue("email")),
		DisplayName: strings.TrimSpace(r.FormValue("display_name")),
		Role:        r.FormValue("role"),
		TelegramID:  strings.TrimSpace(r.FormValue("telegram_id")),
		Password:    r.FormValue("password"),
	}
}

// user turns the form into a user, validated.
func (f userForm) user() (store.User, error) {
	u := store.User{ID: f.ID, Email: f.Email, DisplayName: f.DisplayName, Role: f.Role}
	if at := strings.Index(f.Email, "@"); at < 1 || at == len(f.Email)-1 || strings.ContainsAny(f.Email, " \t") {
		return u, fmt.Errorf("email invalide : %q", f.Email)
	}
	if f.Role != store.UserRoleAdmin && f.Role != store.UserRoleStandard {
		return u, fmt.Errorf("rôle inconnu : %q", f.Role)
	}
	if f.TelegramID != "" {
		id, err := strconv.ParseInt(f.TelegramID, 10, 64)
		if err != nil {
			return u, fmt.Errorf("ID Telegram invalide : %q", f.TelegramID)
		}
		u.TelegramID = &id
	}
	return u, nil
}

func (a *Admin) createUser(w http.ResponseWriter, r *http.Request) {
	f := userFormFromRequest(r)
	f.New = true
	f.ID = uuid.NewString()
	u, err := f.user()
	if err == nil {
		err = auth.CheckPasswordPolicy(f.Password)
	}
	if err == nil {
		u.PasswordHash, err = auth.HashPassword(f.Password)
	}
	if err == nil {
		err = a.cfg.Store.CreateUser(r.Context(), u)
	}
	if err != nil {
		if errors.Is(err, store.ErrUserExists) {
			err = errors.New("cet email ou cet ID Telegram est déjà utilisé")
		}
		f.Error = err.Error()
		f.Password = "" // never sent back to the browser
		a.renderUserForm(w, r, f)
		return
	}
	log.Printf("admin: user %s (%s) created by %s", u.ID, u.Role, auth.UserFrom(r.Context()).ID)
	navigate(w, r, "/admin/users?user=created")
}

func (a *Admin) updateUser(w http.ResponseWriter, r *http.Request) {
	f := userFormFromRequest(r)
	f.ID = chi.URLParam(r, "id")
	u, err := f.user()
	// An admin cannot take the role away from themselves: that is how the
	// last admin locks everyone out. Demoting another admin leaves this one.
	if err == nil && f.ID == auth.UserFrom(r.Context()).ID && f.Role != store.UserRoleAdmin {
		err = errors.New("tu ne peux pas te retirer le rôle admin")
	}
	if err == nil {
		err = a.cfg.Store.UpdateUser(r.Context(), u)
	}
	switch {
	case errors.Is(err, store.ErrUserNotFound):
		http.NotFound(w, r)
	case errors.Is(err, store.ErrUserExists):
		f.Error = "cet email ou cet ID Telegram est déjà utilisé"
		a.renderUserForm(w, r, f)
	case err != nil:
		f.Error = err.Error()
		a.renderUserForm(w, r, f)
	default:
		log.Printf("admin: user %s updated by %s", u.ID, auth.UserFrom(r.Context()).ID)
		navigate(w, r, "/admin/users?user=updated")
	}
}

// resetPassword sets a new password. Every login session of the user ends,
// the admin's own included when it is their account.
func (a *Admin) resetPassword(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	password := r.FormValue("password")
	err := auth.CheckPasswordPolicy(password)
	var hash string
	if err == nil {
		hash, err = auth.HashPassword(password)
	}
	if err == nil {
		err = a.cfg.Store.SetUserPassword(r.Context(), id, hash)
	}
	if errors.Is(err, store.ErrUserNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		u, _ := a.cfg.Store.GetUser(r.Context(), id)
		if u == nil {
			http.NotFound(w, r)
			return
		}
		f := userForm{ID: u.ID, Email: u.Email, DisplayName: u.DisplayName, Role: u.Role, Disabled: u.DisabledAt != nil,
			Error: "Mot de passe non changé : " + err.Error()}
		a.renderUserForm(w, r, f)
		return
	}
	log.Printf("admin: password of user %s reset by %s", id, auth.UserFrom(r.Context()).ID)
	navigate(w, r, "/admin/users?user=password")
}

func (a *Admin) disableUser(w http.ResponseWriter, r *http.Request) { a.setDisabled(w, r, true) }
func (a *Admin) enableUser(w http.ResponseWriter, r *http.Request)  { a.setDisabled(w, r, false) }

func (a *Admin) setDisabled(w http.ResponseWriter, r *http.Request, disabled bool) {
	id := chi.URLParam(r, "id")
	if disabled && id == auth.UserFrom(r.Context()).ID {
		http.Error(w, "Tu ne peux pas désactiver ton propre compte.", http.StatusConflict)
		return
	}
	switch err := a.cfg.Store.SetUserDisabled(r.Context(), id, disabled); {
	case errors.Is(err, store.ErrUserNotFound):
		http.NotFound(w, r)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		state := "enabled"
		if disabled {
			state = "disabled"
		}
		log.Printf("admin: user %s %s by %s", id, state, auth.UserFrom(r.Context()).ID)
		navigate(w, r, "/admin/users?user="+url.QueryEscape(state))
	}
}

var userFlashes = map[string]string{
	"created":  "Utilisateur créé.",
	"updated":  "Utilisateur modifié.",
	"password": "Mot de passe changé : toutes les sessions de cet utilisateur sont fermées.",
	"disabled": "Utilisateur désactivé : il ne peut plus se connecter, et ses sessions de connexion sont fermées.",
	"enabled":  "Utilisateur réactivé.",
}
