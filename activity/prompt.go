package activity

import (
	"sync"

	"github.com/victor/temporal-agent/skill"
)

// Prompts builds an agent's system prompt from the shared catalog and the
// loaded skills. Both change at runtime: a prompt is built from their current
// state, the same for the back-office, a turn's start and each of its calls.
type Prompts struct {
	mu      sync.RWMutex
	skills  map[string]skill.Skill // skill name → skill
	catalog *Catalog
}

func NewPrompts(skills []skill.Skill, catalog *Catalog) *Prompts {
	p := &Prompts{catalog: catalog}
	p.SetSkills(skills)
	return p
}

func (p *Prompts) SetSkills(skills []skill.Skill) {
	byName := make(map[string]skill.Skill, len(skills))
	for _, s := range skills {
		byName[s.Name] = s
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.skills = byName
}

// AgentPrompt returns agentID's base prompt: who it is, behaviors for the
// tools it is offered, then its skills. The agents it may delegate to need no
// section of their own: each agent_<id> tool carries its agent's description.
func (p *Prompts) AgentPrompt(agentID string, tools []string) string {
	var self AgentCatalogEntry
	for _, e := range p.catalog.Agents() {
		if e.ID == agentID {
			self = e
		}
	}
	allowed := make(map[string]bool, len(tools))
	for _, name := range tools {
		allowed[name] = true
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return identitySection(self) + buildSystemPrompt(matchSkills(p.skills, self.Skills), allowed)
}

// userMemorySection is the prompt section holding the memory of the user the
// turn answers. It names that user: in a shared session, the model must not
// take one member's memory for everyone's, nor save the others into it.
func userMemorySection(userName, memory string) string {
	who := "this user"
	if userName != "" {
		who = userName + ", the author of the latest message"
	}
	return "\n## User Memory\n\nThe following is what you remember about " + who +
		" from previous conversations. Use it to personalize your responses. It is private to them: do not reveal it to other participants.\n\n" +
		memory + "\n\n"
}
