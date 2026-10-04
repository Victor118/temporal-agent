// Package chat renders the conversation interface: the tree of sessions, a
// session's thread, its detail rail, and the map of a tree. Building the views
// is pure (this file), so it is tested without HTTP or Temporal; the handlers
// in cmd/agent gather the data and render.
package chat

import (
	"fmt"
	"hash/fnv"
	"html/template"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/store"
)

// Status is what a session is doing, as the session service reads it from
// Temporal.
type Status = session.Status

const (
	StatusIdle    = session.StatusIdle
	StatusWorking = session.StatusWorking
	StatusWaiting = session.StatusWaiting
)

// TreeNode is a session in the tree of forks the user can see.
type TreeNode struct {
	Session  store.Session
	Stats    store.SessionStats
	Status   Status
	Children []*TreeNode
	Parent   *TreeNode
	Depth    int
	Current  bool
	// Orphan: a fork whose parent the user cannot see, shown as a root.
	Orphan bool
	// lastActivity is the latest activity in the subtree, to order roots.
	lastActivity time.Time
}

// Title is the session's title, or a stand-in.
func (n *TreeNode) Title() string {
	if n.Session.Title != "" {
		return n.Session.Title
	}
	return "Session sans titre"
}

// BuildTree arranges the user's sessions as trees of forks. A fork hangs under
// its parent when the user can see the parent, and stands as a root
// otherwise. Roots come most recently active first, children in fork order.
func BuildTree(sessions []store.Session, stats map[string]store.SessionStats, statuses map[string]Status, currentID string) []*TreeNode {
	nodes := make(map[string]*TreeNode, len(sessions))
	for _, s := range sessions {
		st := stats[s.SessionID]
		status := statuses[s.SessionID]
		if status == "" {
			status = StatusIdle
		}
		nodes[s.SessionID] = &TreeNode{Session: s, Stats: st, Status: status, Current: s.SessionID == currentID, lastActivity: st.LastActivity}
	}

	var roots []*TreeNode
	for _, s := range sessions {
		n := nodes[s.SessionID]
		if p, ok := nodes[s.ParentSessionID]; ok && p != n {
			n.Parent = p
			p.Children = append(p.Children, n)
			continue
		}
		n.Orphan = s.ParentSessionID != ""
		roots = append(roots, n)
	}

	var settle func(n *TreeNode, depth int) time.Time
	settle = func(n *TreeNode, depth int) time.Time {
		n.Depth = depth
		sort.Slice(n.Children, func(i, j int) bool {
			a, b := n.Children[i].Session, n.Children[j].Session
			if a.ForkedAtMessageID != b.ForkedAtMessageID {
				return a.ForkedAtMessageID < b.ForkedAtMessageID
			}
			return a.CreatedAt.Before(b.CreatedAt)
		})
		for _, c := range n.Children {
			if t := settle(c, depth+1); t.After(n.lastActivity) {
				n.lastActivity = t
			}
		}
		return n.lastActivity
	}
	for _, r := range roots {
		settle(r, 0)
	}
	sort.SliceStable(roots, func(i, j int) bool { return roots[i].lastActivity.After(roots[j].lastActivity) })
	return roots
}

// Find returns the node of a session in the trees, or nil.
func Find(roots []*TreeNode, id string) *TreeNode {
	for _, r := range roots {
		if r.Session.SessionID == id {
			return r
		}
		if n := Find(r.Children, id); n != nil {
			return n
		}
	}
	return nil
}

// Path returns the nodes from the root down to n.
func Path(n *TreeNode) []*TreeNode {
	var path []*TreeNode
	for ; n != nil; n = n.Parent {
		path = append([]*TreeNode{n}, path...)
	}
	return path
}

// Root returns the root of n's tree.
func Root(n *TreeNode) *TreeNode {
	for n != nil && n.Parent != nil {
		n = n.Parent
	}
	return n
}

// Count returns the number of sessions in the tree under n, n included.
func Count(n *TreeNode) int {
	c := 1
	for _, ch := range n.Children {
		c += Count(ch)
	}
	return c
}

// Person is a user as the interface shows them.
type Person struct {
	ID       string
	Name     string
	Initials string
	Color    string
}

// avatarColors are the avatar backgrounds, picked from the user ID so a user
// keeps theirs.
var avatarColors = []string{"#0F6E67", "#7A5C2E", "#4A4F6B", "#6B3FA0", "#1F4E79", "#8A5A17", "#5B6B2E", "#7A3B4F"}

// NewPerson builds the avatar of a user from their ID and name.
func NewPerson(id, name string) Person {
	h := fnv.New32a()
	h.Write([]byte(id))
	return Person{ID: id, Name: name, Initials: initials(name), Color: avatarColors[h.Sum32()%uint32(len(avatarColors))]}
}

// initials are the first letters of the first two words of a name, or the
// first two letters of a single word (an email, say).
func initials(name string) string {
	name = strings.Split(name, "@")[0]
	words := strings.FieldsFunc(name, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	switch {
	case len(words) == 0:
		return "?"
	case len(words) == 1:
		r := []rune(words[0])
		if len(r) > 2 {
			r = r[:2]
		}
		return strings.ToUpper(string(r))
	default:
		return strings.ToUpper(string([]rune(words[0])[0]) + string([]rune(words[1])[0]))
	}
}

// ForkLink is a fork of a message.
type ForkLink struct {
	SessionID string
	Title     string
}

// Question is a question an agent waits on, for the session's members.
type Question = session.Question

// Item kinds of a thread.
const (
	ItemHuman    = "human"
	ItemAgent    = "agent"
	ItemBrief    = "brief"
	ItemQuestion = "question"
	ItemError    = "error"    // why a turn failed
	ItemReport   = "report"   // a fork's report to this session
	ItemReported = "reported" // in a fork: where its latest report stopped
)

// ReportLink is the other end of a report: in the parent, the fork it comes
// from; in the fork, the parent it went to.
type ReportLink struct {
	SessionID string
	Title     string // the fork's title when it reported
	MessageID int64  // in the fork: the report's message in the parent
	// Accessible: the viewer is a member of that session, which still exists.
	Accessible bool
}

// ThreadItem is one entry of a thread: a member's message, an agent's answer
// with the tools it used, the brief a fork started from, or a pending
// question.
type ThreadItem struct {
	Kind string
	// ID is the message to fork from: for an agent answer, its last message.
	ID   int64
	Time time.Time

	Author Person // human, report: who wrote or sent it
	Mine   bool   // human: written by the viewer
	Text   string // human: plain text; question: the question; error: the reason

	HTML   template.HTML // agent answer, brief or report, rendered from Markdown
	Report ReportLink    // report: the fork it comes from; reported: the parent
	Tools  []string      // agent: the tools the answer used, in order, once each
	Agent  AgentInfo     // agent answer, error: the agent that wrote it

	Forks []ForkLink // forks started from this item

	WorkflowID string   // question
	AgentChain []string // question: the agents that led to it
}

// AgentDirectory names the agents of a thread.
type AgentDirectory struct {
	ByID    map[string]AgentInfo // the installation's agents, as they are now
	Session AgentInfo            // the session's agent
}

// Signer is the agent that wrote m: as it is now; under the name it had then
// once it is gone; the session's agent for a message no agent signed.
func (d AgentDirectory) Signer(m store.Message) AgentInfo {
	if m.AgentID == "" {
		return d.Session
	}
	if a, ok := d.ByID[m.AgentID]; ok {
		return a
	}
	name := m.Author
	if name == "" {
		name = m.AgentID
	}
	return AgentInfo{ID: m.AgentID, Name: name}
}

// BuildThread turns a session's messages into thread items. An agent's turn,
// spread over several messages (tool calls, results, text), becomes one item,
// signed by its agent: when several agents answer one after another, each has
// its own.
//
// forks are the session's forks the viewer is a member of, by the message they
// started from: a report links to its fork only if it is one of them.
func BuildThread(msgs []store.MessageWithID, viewerID string, forks map[int64][]ForkLink, questions []Question, agents AgentDirectory) []ThreadItem {
	visible := map[string]bool{}
	for _, links := range forks {
		for _, f := range links {
			visible[f.SessionID] = true
		}
	}
	var items []ThreadItem
	var agent *ThreadItem // the agent item being assembled
	var agentText []string
	closeAgent := func() {
		if agent == nil {
			return
		}
		agent.HTML = Markdown(strings.Join(agentText, "\n\n"))
		items = append(items, *agent)
		agent, agentText = nil, nil
	}

	for _, m := range msgs {
		switch {
		case m.Kind == store.KindForkSummary:
			closeAgent()
			items = append(items, ThreadItem{Kind: ItemBrief, ID: m.ID, Time: m.CreatedAt, HTML: Markdown(text(m.Content))})
		case m.Kind == store.KindTurnEnd:
			// A turn's end shows only when it says why the turn failed; it
			// still closes the turn's item.
			closeAgent()
			if reason := store.TurnEndError(m.Message); reason != "" {
				items = append(items, ThreadItem{Kind: ItemError, ID: m.ID, Time: m.CreatedAt, Text: reason, Agent: agents.Signer(m.Message)})
			}
		case m.Kind == store.KindForkReport:
			closeAgent()
			items = append(items, ThreadItem{
				Kind: ItemReport, ID: m.ID, Time: m.CreatedAt,
				Author: NewPerson(m.UserID, authorName(m)), Mine: m.UserID != "" && m.UserID == viewerID,
				HTML: Markdown(text(m.Content)), Report: reportSource(m.Fork, visible),
			})
		case m.Role == store.RoleUser:
			closeAgent()
			items = append(items, ThreadItem{
				Kind: ItemHuman, ID: m.ID, Time: m.CreatedAt,
				Author: NewPerson(m.UserID, authorName(m)), Mine: m.UserID != "" && m.UserID == viewerID,
				Text: text(m.Content),
			})
		case m.Role == store.RoleAssistant:
			signer := agents.Signer(m.Message)
			if agent != nil && agent.Agent.ID != signer.ID {
				closeAgent()
			}
			if agent == nil {
				agent = &ThreadItem{Kind: ItemAgent, Time: m.CreatedAt, Agent: signer}
			}
			for _, tc := range m.ToolCalls {
				if !contains(agent.Tools, tc.Name) {
					agent.Tools = append(agent.Tools, tc.Name)
				}
			}
			if t := text(m.Content); strings.TrimSpace(t) != "" {
				agentText = append(agentText, t)
				agent.ID = m.ID
			}
			if agent.ID == 0 {
				agent.ID = m.ID
			}
		}
		// Tool results are not shown: the tools appear on the answer.
	}
	closeAgent()

	for i := range items {
		items[i].Forks = forks[items[i].ID]
	}
	for _, q := range questions {
		items = append(items, ThreadItem{Kind: ItemQuestion, Text: q.Text, WorkflowID: q.WorkflowID, AgentChain: q.AgentChain})
	}
	return items
}

// reportSource is the fork a report names, linked when the viewer can open it.
func reportSource(f *store.ForkRef, visible map[string]bool) ReportLink {
	if f == nil {
		return ReportLink{Title: "Fork"}
	}
	title := f.Title
	if title == "" {
		title = "Fork"
	}
	return ReportLink{SessionID: f.SessionID, Title: title, Accessible: visible[f.SessionID]}
}

// MarkReported shows, in a fork's thread, where its latest report stopped:
// after the last item it covers, before the questions waiting. The parent is
// linked when the viewer is a member of it. A fork that never reported is
// left as it is.
func MarkReported(items []ThreadItem, fork store.Session, parentVisible bool) []ThreadItem {
	if fork.LastReportedMessageID == 0 || fork.LastReportedAt == nil {
		return items
	}
	at := 0
	for i, it := range items {
		if it.ID != 0 && it.ID <= fork.LastReportedMessageID {
			at = i + 1
		}
	}
	mark := ThreadItem{Kind: ItemReported, Time: *fork.LastReportedAt, Report: ReportLink{
		SessionID: fork.ParentSessionID, MessageID: fork.LastReportID, Accessible: parentVisible && fork.ParentSessionID != "",
	}}
	return append(items[:at:at], append([]ThreadItem{mark}, items[at:]...)...)
}

func authorName(m store.MessageWithID) string {
	if m.Author != "" {
		return m.Author
	}
	return "Utilisateur"
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// MapNode is a session placed on the map.
type MapNode struct {
	*TreeNode
	X, Y int
}

// MapView is a tree laid out for the map: nodes in levels, parents centred
// over their children, and the connectors between them as SVG paths.
type MapView struct {
	Root   *TreeNode
	Nodes  []MapNode
	Edges  []string
	Width  int
	Height int
}

// Map geometry, in pixels.
const (
	MapNodeW  = 220
	MapNodeH  = 92
	mapGapX   = 28
	mapLevelH = 160
	mapMargin = 40
)

// BuildMap lays out the tree under root: each leaf takes the next slot from
// the left, and a parent sits centred over its first and last child.
func BuildMap(root *TreeNode) MapView {
	v := MapView{Root: root}
	slot, maxDepth := 0, 0
	pos := map[*TreeNode]MapNode{}
	var place func(n *TreeNode, depth int) int
	place = func(n *TreeNode, depth int) int {
		if depth > maxDepth {
			maxDepth = depth
		}
		var x int
		if len(n.Children) == 0 {
			x = mapMargin + slot*(MapNodeW+mapGapX)
			slot++
		} else {
			first := place(n.Children[0], depth+1)
			last := first
			for _, c := range n.Children[1:] {
				last = place(c, depth+1)
			}
			x = (first + last) / 2
		}
		pos[n] = MapNode{TreeNode: n, X: x, Y: mapMargin + depth*mapLevelH}
		v.Nodes = append(v.Nodes, pos[n])
		return x
	}
	place(root, 0)

	for _, mn := range v.Nodes {
		for _, c := range mn.Children {
			cn := pos[c]
			x1, y1 := mn.X+MapNodeW/2, mn.Y+MapNodeH
			x2, y2 := cn.X+MapNodeW/2, cn.Y
			mid := (y1 + y2) / 2
			v.Edges = append(v.Edges, pathf(x1, y1, mid, x2, y2))
		}
	}
	// Parents were appended after their children: draw from the root down.
	sort.SliceStable(v.Nodes, func(i, j int) bool { return v.Nodes[i].Depth < v.Nodes[j].Depth })
	v.Width = mapMargin*2 + slot*(MapNodeW+mapGapX) - mapGapX
	v.Height = mapMargin*2 + maxDepth*mapLevelH + MapNodeH
	return v
}

// pathf is an orthogonal connector: down from the parent, across, down to the
// child.
func pathf(x1, y1, mid, x2, y2 int) string {
	return fmt.Sprintf("M%d %d V%d H%d V%d", x1, y1, mid, x2, y2)
}
