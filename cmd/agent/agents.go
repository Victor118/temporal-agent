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
			ID:           def.ID,
			Name:         def.Name,
			Description:  def.Description,
			Skills:       def.Skills,
			Tools:        def.Tools,
			DefaultQueue: def.DefaultQueue,
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

// loadCatalogFromDB reads the full agent catalog from PostgreSQL.
func loadCatalogFromDB(st store.Store) ([]activity.AgentCatalogEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	agents, err := st.ListAgents(ctx)
	if err != nil {
		return nil, err
	}

	catalog := make([]activity.AgentCatalogEntry, len(agents))
	for i, a := range agents {
		catalog[i] = activity.AgentCatalogEntry{
			ID:           a.ID,
			Name:         a.Name,
			Description:  a.Description,
			Skills:       a.Skills,
			Tools:        a.Tools,
			DefaultQueue: a.DefaultQueue,
		}
	}
	return catalog, nil
}

// initCatalog loads the agent catalog at startup and logs it.
func initCatalog(st store.Store) []activity.AgentCatalogEntry {
	catalog, err := loadCatalogFromDB(st)
	if err != nil {
		log.Printf("Warning: failed to load agents catalog from DB: %v", err)
		return nil
	}
	if len(catalog) == 0 {
		log.Println("Warning: agents catalog is empty")
	}
	for _, a := range catalog {
		tools := "all"
		if a.Tools != nil {
			tools = fmt.Sprint(a.Tools)
		}
		log.Printf("Agent %q (queue=%s): skills %v, tools %s", a.ID, a.DefaultQueue, a.Skills, tools)
	}
	return catalog
}

// pollAgents periodically reloads the agent catalog from DB into skillAct.
// current is the catalog already loaded at startup; changes are logged.
func pollAgents(ctx context.Context, st store.Store, skillAct *activity.SkillActivities, current []activity.AgentCatalogEntry, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			catalog, err := loadCatalogFromDB(st)
			if err != nil {
				log.Printf("Warning: failed to poll agents catalog from DB: %v", err)
				continue
			}
			if reflect.DeepEqual(catalog, current) {
				continue
			}
			current = catalog
			activity.SetCatalog(skillAct, catalog)
			log.Printf("Agents catalog changed, reloaded %d agents", len(catalog))
		}
	}
}
