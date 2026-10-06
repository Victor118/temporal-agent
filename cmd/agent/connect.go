package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/machine/connect"
)

// version is the binary's version, which a machine announces; set at build
// time (-ldflags "-X main.version=…").
var version = "dev"

// connectCmd is `agent connect`: this machine, its owner's, runs the
// directives the server sends it. It opens neither the database nor
// Temporal: it holds a machine token, and a WebSocket to the server.
var connectCmd = &cobra.Command{
	Use:   "connect",
	Short: "Connect this machine to a server, to run what its owner's agents send it (no database, no Temporal)",
	Long: `Connects this machine to a server's gateway and runs the directives it sends.

The first time, enroll it: agent connect --join https://agent.example.com
shows a code to type in the server's "Mes machines" page. From a script,
--token-stdin reads an enrollment token (created on that page) from the
standard input instead. The machine's token is then kept in its directory
(0600), and agent connect reconnects by itself.`,
	Run: runConnect,
}

func init() {
	f := connectCmd.Flags()
	f.String("join", "", "enroll this machine on the server at this URL (https://…)")
	f.String("name", "", "the machine's name, at enrollment (default: the host name)")
	f.Bool("token-stdin", false, "with --join: read an enrollment token from the standard input instead of showing a code")
	f.String("dir", "", "the machine's directory (default: $XDG_CONFIG_HOME/agent/machine)")
	f.Int("max-directives", 1, "how many directives this machine runs at once")
}

func runConnect(cmd *cobra.Command, args []string) {
	join, _ := cmd.Flags().GetString("join")
	name, _ := cmd.Flags().GetString("name")
	tokenStdin, _ := cmd.Flags().GetBool("token-stdin")
	dir, _ := cmd.Flags().GetString("dir")
	maxDirectives, _ := cmd.Flags().GetInt("max-directives")
	if maxDirectives < 1 || maxDirectives > machine.MaxDirectives {
		log.Fatalf("--max-directives: between 1 and %d", machine.MaxDirectives)
	}
	if dir == "" {
		var err error
		if dir, err = connect.DefaultDir(); err != nil {
			log.Fatalf("The machine's directory: %v", err)
		}
	}
	state := connect.State{Dir: dir}
	if err := state.Init(); err != nil {
		log.Fatalf("The machine's directory %s: %v", dir, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	executors := map[string]connect.Executor{machine.KindEcho: connect.Echo}
	if tokenStdin && join == "" {
		log.Fatal("--token-stdin goes with --join")
	}
	if join != "" {
		if cfg, err := state.Load(); err == nil {
			log.Fatalf("This machine is already enrolled as %q on %s. To enroll it again, revoke it in « Mes machines » and remove %s",
				cfg.Name, cfg.Server, dir)
		}
		if err := enroll(ctx, state, join, name, tokenStdin, executors, maxDirectives); err != nil {
			log.Fatal(err)
		}
	}

	c := &connect.Client{State: state, Executors: executors, MaxDirectives: maxDirectives, OS: runtime.GOOS + "/" + runtime.GOARCH, Version: version}
	cfg, err := state.Load()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("connect: machine %q of %s, running %v, %d at a time (%s)", cfg.Name, cfg.Server, c.Capabilities(), maxDirectives, dir)
	if err := c.Run(ctx); err != nil {
		if errors.Is(err, connect.ErrRefused) {
			log.Fatalf("Stopped: %v", err)
		}
		log.Fatal(err)
	}
	log.Println("connect: stopped")
}

// enroll enrolls the machine, by a code its owner types in the front, or by
// an enrollment token read from the standard input (never an argument: it
// would show in the process list and the shell's history).
func enroll(ctx context.Context, state connect.State, join, name string, tokenStdin bool, executors map[string]connect.Executor, maxDirectives int) error {
	server, err := connect.CheckServer(join)
	if err != nil {
		return err
	}
	if name == "" {
		if name, err = os.Hostname(); err != nil {
			name = "machine"
		}
	}
	en := &connect.Enroller{Server: server, Out: os.Stdout}
	req := machine.EnrollRequest{Name: name, OS: runtime.GOOS + "/" + runtime.GOARCH, MaxDirectives: maxDirectives, AgentVersion: version}
	for kind := range executors {
		req.Capabilities = append(req.Capabilities, kind)
	}
	var grant machine.TokenGrant
	if tokenStdin {
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && line == "" {
			return fmt.Errorf("read the enrollment token from the standard input: %w", err)
		}
		req.EnrollmentToken = strings.TrimSpace(line)
		grant, err = en.ByToken(ctx, req)
		if err != nil {
			return err
		}
	} else if grant, err = en.ByCode(ctx, req); err != nil {
		return err
	}
	if err := state.Save(connect.Config{Server: server, MachineID: grant.MachineID, Name: grant.Name, Token: grant.Token}); err != nil {
		return fmt.Errorf("keep the machine's token: %w", err)
	}
	fmt.Printf("Machine « %s » inscrite sur %s.\n", grant.Name, server)
	return nil
}
