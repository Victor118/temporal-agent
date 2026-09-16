package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/store"
)

// seedAgents imports agents from the seed file that don't exist in the DB yet.
// Existing agents are never modified: the DB is the source of truth.
// A missing seed file is not an error; an invalid one is.
func seedAgents(st store.Store, path string) error {
	defs, err := config.LoadAgentDefinitions(path)
	if errors.Is(err, os.ErrNotExist) {
		log.Printf("No agents seed file at %s, skipping seed", path)
		return nil
	}
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, def := range defs {
		inserted, err := st.InsertAgentIfAbsent(ctx, store.Agent{
			ID:          def.ID,
			Name:        def.Name,
			Description: def.Description,
			Skills:      def.Skills,
			Tools:       def.Tools,
		})
		if err != nil {
			return err
		}
		if inserted {
			log.Printf("Seeded agent %q from %s", def.ID, path)
		}
	}
	return nil
}

// loadAgentsFromDB reads the full agent catalog from PostgreSQL.
func loadAgentsFromDB(ctx context.Context, st store.Store) ([]activity.AgentCatalogEntry, error) {
	agents, err := st.ListAgents(ctx)
	if err != nil {
		return nil, err
	}

	catalog := make([]activity.AgentCatalogEntry, len(agents))
	for i, a := range agents {
		catalog[i] = activity.AgentCatalogEntry{
			ID:          a.ID,
			Name:        a.Name,
			Description: a.Description,
			Skills:      a.Skills,
			Tools:       a.Tools,
		}
	}
	return catalog, nil
}

// initCatalog creates the worker catalog, loads it from DB and logs the agents.
func initCatalog(st store.Store) *activity.Catalog {
	catalog := activity.NewCatalog()
	refreshCatalog(st, catalog)

	agents := catalog.Agents()
	if len(agents) == 0 {
		log.Println("Warning: agents catalog is empty")
	}
	for _, a := range agents {
		tools := "all"
		if a.Tools != nil {
			tools = fmt.Sprint(a.Tools)
		}
		log.Printf("Agent %q: skills %v, tools %s", a.ID, a.Skills, tools)
	}
	return catalog
}

// refreshCatalog reloads agents and tools from DB into catalog, logging changes.
// On a DB error the previous content is kept.
func refreshCatalog(st store.Store, catalog *activity.Catalog) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if agents, err := loadAgentsFromDB(ctx, st); err != nil {
		log.Printf("Warning: failed to load agents catalog from DB: %v", err)
	} else if !reflect.DeepEqual(agents, catalog.Agents()) {
		catalog.SetAgents(agents)
		log.Printf("Agents catalog loaded: %d agents", len(agents))
	}

	if tools, err := st.ListTools(ctx); err != nil {
		log.Printf("Warning: failed to load tools catalog from DB: %v", err)
	} else if !reflect.DeepEqual(tools, catalog.Tools()) {
		catalog.SetTools(tools)
		log.Printf("Tools catalog loaded: %d tools", len(tools))
	}
}

// pollCatalog periodically refreshes the worker catalog from DB.
func pollCatalog(ctx context.Context, st store.Store, catalog *activity.Catalog, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCatalog(st, catalog)
		}
	}
}
