package activity

import (
	"sync"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/skill"
)

// Prompts builds an agent's system prompt from the shared catalog and the
// loaded skills. Both change at runtime: a prompt is built from their current
// state, the same for the back-office, a turn's start and each of its calls.
type Prompts struct {
	mu     sync.RWMutex
	skills map[string]skill.Skill // skill name → skill
	// version is what the skills were loaded from (skill.Version): a
	// repository's commit, or empty.
	version string
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

// SetVersion records what the skills were loaded from (skill.Version).
func (p *Prompts) SetVersion(version string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.version = version
}

// RunSkillSet is what a coding run takes along of the skills named for it
// (RunSkillReader): those found, the names not found, and the version they
// were loaded from.
type RunSkillSet struct {
	Skills  []machine.RunSkill
	Missing []string
	Version string
}

// RunSkillReader reads, by name, the skills a coding run takes along: the
// ones of this worker marked "runs: true" (docs/design/run-skills.md).
type RunSkillReader interface {
	RunSkills(names []string) RunSkillSet
}

// RunSkills reads the skills named, marked "runs: true", each once, in
// order: a name it has not, or not so marked, is missing.
func (p *Prompts) RunSkills(names []string) RunSkillSet {
	p.mu.RLock()
	defer p.mu.RUnlock()
	set := RunSkillSet{Version: p.version}
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		s, ok := p.skills[name]
		if !ok || !s.Runs {
			set.Missing = append(set.Missing, name)
			continue
		}
		set.Skills = append(set.Skills, machine.RunSkill{Name: s.Name, Description: s.Description, Content: s.Content})
	}
	return set
}

// runSkillNames are those of names whose skill is marked "runs: true".
func (p *Prompts) runSkillNames(names []string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var runs []string
	for _, name := range names {
		if s, ok := p.skills[name]; ok && s.Runs {
			runs = append(runs, name)
		}
	}
	return runs
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
