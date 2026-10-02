package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/store"
)

// The user commands work on the database directly: they are how the first
// admin is created, before any back-office can be logged into.
var userCmd = &cobra.Command{
	Use:   "user",
	Short: "Manage user accounts",
}

var userFlags struct {
	email, name  string
	admin, stdin bool
}

var userCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a user account (asks for the password)",
	Example: `  agent user create --email victor@example.com --name Victor --admin
  echo "$PASSWORD" | agent user create --email bot@example.com --password-stdin`,
	RunE: func(cmd *cobra.Command, args []string) error {
		st, err := openUserStore()
		if err != nil {
			return err
		}
		defer st.Close()

		password, err := readNewPassword(userFlags.stdin)
		if err != nil {
			return err
		}
		hash, err := auth.HashPassword(password)
		if err != nil {
			return err
		}
		role := store.UserRoleStandard
		if userFlags.admin {
			role = store.UserRoleAdmin
		}
		u := store.User{ID: newUUID(), Email: userFlags.email, DisplayName: userFlags.name, Role: role, PasswordHash: hash}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := st.CreateUser(ctx, u); err != nil {
			return err
		}
		fmt.Printf("Created %s %s (%s)\n", role, u.Email, u.ID)
		return nil
	},
}

var userPasswordCmd = &cobra.Command{
	Use:   "set-password",
	Short: "Set a user's password (asks for it) and log them out everywhere",
	RunE: func(cmd *cobra.Command, args []string) error {
		st, err := openUserStore()
		if err != nil {
			return err
		}
		defer st.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		u, err := st.GetUserByEmail(ctx, userFlags.email)
		if err != nil {
			return err
		}
		if u == nil {
			return fmt.Errorf("no user with email %q", userFlags.email)
		}
		password, err := readNewPassword(userFlags.stdin)
		if err != nil {
			return err
		}
		hash, err := auth.HashPassword(password)
		if err != nil {
			return err
		}
		if err := st.SetUserPassword(ctx, u.ID, hash); err != nil {
			return err
		}
		fmt.Printf("Password of %s changed\n", u.Email)
		return nil
	},
}

func init() {
	for _, c := range []*cobra.Command{userCreateCmd, userPasswordCmd} {
		c.Flags().StringVar(&userFlags.email, "email", "", "login email (required)")
		c.Flags().BoolVar(&userFlags.stdin, "password-stdin", false, "read the password from stdin instead of prompting")
		c.MarkFlagRequired("email")
	}
	userCreateCmd.Flags().StringVar(&userFlags.name, "name", "", "display name")
	userCreateCmd.Flags().BoolVar(&userFlags.admin, "admin", false, "give the admin role (back-office access)")
	userCmd.AddCommand(userCreateCmd, userPasswordCmd)
}

func openUserStore() (*store.PostgresStore, error) {
	return store.NewPostgresStore(config.Load().DatabaseURL)
}

// readNewPassword prompts twice without echo on a terminal, or reads one line
// from stdin with --password-stdin. Never a flag: it would land in the shell
// history and the process list.
func readNewPassword(fromStdin bool) (string, error) {
	var password string
	if fromStdin {
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && line == "" {
			return "", fmt.Errorf("read password: %w", err)
		}
		password = strings.TrimRight(line, "\r\n")
	} else {
		if !term.IsTerminal(int(os.Stdin.Fd())) {
			return "", errors.New("no terminal to ask the password on: use --password-stdin (and docker compose exec -it)")
		}
		first, err := promptPassword("Password: ")
		if err != nil {
			return "", err
		}
		second, err := promptPassword("Again: ")
		if err != nil {
			return "", err
		}
		if first != second {
			return "", errors.New("the two passwords differ")
		}
		password = first
	}
	return password, auth.CheckPasswordPolicy(password)
}

func promptPassword(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt)
	b, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	return string(b), err
}
