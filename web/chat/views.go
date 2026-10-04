// Package chat renders the conversation interface: the tree of sessions, a
// session's thread, its detail rail, and the map of a tree. Building the views
// is pure (this file), so it is tested without HTTP or Temporal; the handlers
// in cmd/agent gather the data and render.
package chat

import (
	"cmp"
	"fmt"
	"hash/fnv"
	"html/template"
	"slices"
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
	// ItemFiles: the files of a turn the thread does not show yet (it
	// wrote nothing so far), on their own until it does (AttachFiles).
	ItemFiles = "files"
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
	// ID names the item's element (m<ID>): for an agent answer, its last
	// text, so a block keeps its element while it runs without writing.
	ID int64
	// ForkID is the message a fork from this item goes up to: for a turn
	// that ended, its end, so the fork takes all the thread shows above
	// the block (a turn shows where it ended); ID otherwise.
	ForkID int64
	// Time is when the item was written: for an agent's turn, when it
	// started. End is when it ended, if not in the same minute (start →
	// end); zero otherwise, and while it runs.
	Time time.Time
	End  time.Time

	Author Person // human, report: who wrote or sent it
	Mine   bool   // human: written by the viewer
	Text   string // human: plain text; question: the question; error: the reason

	HTML   template.HTML // agent answer, brief or report, rendered from Markdown
	Report ReportLink    // report: the fork it comes from; reported: the parent
	Tools  []string      // agent: the tools the answer used, in order, once each
	Agent  AgentInfo     // agent answer, error, files: the agent that wrote it
	// Files are the files the turn published (AttachFiles): on its answer,
	// or on its error when it shows none.
	Files []FileLink

	// Running: an agent's turn not ended yet, shown at the thread's end
	// (« 16:27 → en cours »).
	Running bool
	// Stopped: an agent's turn that will never end, its participant having
	// gone on to another (« 16:27 → interrompu »).
	Stopped bool
	// Quote: the message an agent's turn answers, when the thread shows
	// something else between them; nil when the turn follows it.
	Quote *Quote

	Forks []ForkLink // forks started from this item

	WorkflowID string   // question
	AgentChain []string // question: the agents that led to it

	turn string // agent answer, error: the turn it shows ("" for none)
}

// Quote is the message a turn answers, as its item recalls it: a link to
// it, whom it is from, and its text on one line.
type Quote struct {
	Target string // the element of the quoted message: "m<id>", "brief"
	Label  string // "en réponse à Victor", "en réponse au brief"
	Text   string // clipped to maxQuoteRunes, on one line; may be empty
}

// maxQuoteRunes bounds a quote's text: one line, the start of the question.
const maxQuoteRunes = 120

// AgentDirectory names the agents of a thread.
type AgentDirectory struct {
	ByID    map[string]AgentInfo // the installation's agents, as they are now
	List    []AgentInfo          // the same, in the store's order
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
// The thread is chronological (chronological): the messages no turn wrote
// in the order of their IDs, each turn whole where it ended, the turns
// still running at the end. Participants answer in parallel: Smith's
// review, started before Victor asked Jarvis and ended after Jarvis
// answered, shows below Jarvis's answer. A turn shown away from the message
// it answers quotes it (Quote). The model still reads the conversation in
// the order of the anchors (conversation.Order).
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
	ordered, states := chronological(msgs)
	items := threadItems(ordered, states, viewerID, visible, agents)
	for i := range items {
		it := &items[i]
		it.Forks = forks[it.ID]
		if it.ForkID != it.ID {
			// Forks are made from ForkID; older ones, from the last text.
			it.Forks = slices.Concat(it.Forks, forks[it.ForkID])
		}
	}
	for _, q := range questions {
		items = append(items, ThreadItem{Kind: ItemQuestion, Text: q.Text, WorkflowID: q.WorkflowID, AgentChain: q.AgentChain})
	}
	return items
}

// turnState is where a turn without an end stands; a turn with one is
// absent from the states chronological returns.
type turnState int

const (
	turnRunning turnState = iota + 1 // still running: shown at the end
	turnStopped                      // will never end: shown where it stopped
)

// chronological orders a session's messages as its thread shows them, and
// tells which turns have no end, running or stopped:
//   - a message no turn wrote (a member's, a fork's summary or report, a
//     scheduled result) at its ID;
//   - a turn, as one block in the order it wrote, at the ID of its end
//     (store.TurnEndKey): where it was finished;
//   - a turn without an end at the thread's end, in the order of their
//     first message: it still runs.
//
// A participant answers one message at a time: a turn without an end
// followed by another turn of its participant will never end (its
// participant was stopped from outside before writing it). It is stopped:
// it shows where it stopped, at its last message, and does not run. Only a
// turn that wrote something besides its end counts as following: an end
// alone may be written by another (a relay that failed, a queue cleared, a
// turn refused by its check) and does not prove its participant went on.
// The price: a dead turn followed only by such ends still shows running.
//
// When each message is written once the answers to the previous one have
// ended, this is the order of the anchors (conversation.Order).
func chronological(msgs []store.MessageWithID) ([]store.MessageWithID, map[string]turnState) {
	type block struct {
		at, first, last int64
		ended           bool
		wrote           bool // a turn's: holds a message besides its end
		msgs            []store.MessageWithID
	}
	byTurn := map[string]*block{}
	var blocks []*block
	for _, m := range msgs {
		turn, ok := store.TurnOf(m.Key)
		if !ok {
			blocks = append(blocks, &block{at: m.ID, ended: true, msgs: []store.MessageWithID{m}})
			continue
		}
		b := byTurn[turn]
		if b == nil {
			b = &block{first: m.ID, last: m.ID}
			byTurn[turn] = b
			blocks = append(blocks, b)
		}
		b.first, b.last = min(b.first, m.ID), max(b.last, m.ID)
		if store.IsTurnEnd(m.Key) {
			b.at, b.ended = m.ID, true
		} else {
			b.wrote = true
		}
		b.msgs = append(b.msgs, m)
	}
	latest := map[string]string{} // participant: its latest turn that wrote
	for turn, b := range byTurn {
		if !b.wrote {
			continue
		}
		p := store.TurnParticipant(turn)
		if prev, ok := latest[p]; !ok || byTurn[prev].first < b.first {
			latest[p] = turn
		}
	}
	states := map[string]turnState{}
	for turn, b := range byTurn {
		switch {
		case b.ended:
		case latest[store.TurnParticipant(turn)] == turn:
			states[turn] = turnRunning
		default:
			states[turn] = turnStopped
			b.at, b.ended = b.last, true // shown where it stopped
		}
	}
	slices.SortStableFunc(blocks, func(a, b *block) int {
		switch {
		case a.ended != b.ended:
			if a.ended {
				return -1
			}
			return 1
		case a.ended:
			return cmp.Compare(a.at, b.at)
		}
		return cmp.Compare(a.first, b.first)
	})
	out := make([]store.MessageWithID, 0, len(msgs))
	for _, b := range blocks {
		slices.SortStableFunc(b.msgs, func(x, y store.MessageWithID) int { return cmp.Compare(x.ID, y.ID) })
		out = append(out, b.msgs...)
	}
	return out, states
}

// threadItems turns messages, in the order the thread shows them, into its
// items: a turn's messages, contiguous, make one answer, and its end the
// error it failed on, if any. A turn's first item quotes the message it
// answers when that is not the item before.
func threadItems(ordered []store.MessageWithID, states map[string]turnState, viewerID string, visible map[string]bool, agents AgentDirectory) []ThreadItem {
	var items []ThreadItem
	at := map[int64]int{}                    // a message no turn wrote: its item
	plain := map[int64]store.MessageWithID{} // those messages, to quote them
	var agent *ThreadItem                    // the agent item being assembled
	var agentText []string
	var agentPlain []int64 // the messages no turn wrote in it (a scheduled result)
	closeAgent := func() {
		if agent == nil {
			return
		}
		agent.HTML = Markdown(strings.Join(agentText, "\n\n"))
		for _, id := range agentPlain {
			at[id] = len(items)
		}
		items = append(items, *agent)
		agent, agentText, agentPlain = nil, nil, nil
	}
	add := func(m store.MessageWithID, it ThreadItem) {
		closeAgent()
		at[m.ID] = len(items)
		plain[m.ID] = m
		items = append(items, it)
	}

	for _, m := range ordered {
		turn, _ := store.TurnOf(m.Key)
		switch {
		case m.Kind == store.KindForkSummary:
			add(m, ThreadItem{Kind: ItemBrief, ID: m.ID, Time: m.CreatedAt, HTML: Markdown(text(m.Content))})
		case m.Kind == store.KindTurnEnd:
			// A turn's end shows only when it says why the turn failed; it
			// still closes the turn's item, which is its own: a turn's end
			// is the last of its block. A fork from the answer goes up to
			// it, and the answer shows when it ended.
			if agent != nil && agent.turn == turn && turn != "" {
				agent.ForkID = m.ID
				if !sameMinute(agent.Time, m.CreatedAt) {
					agent.End = m.CreatedAt
				}
			}
			closeAgent()
			if reason := store.TurnEndError(m.Message); reason != "" {
				items = append(items, ThreadItem{Kind: ItemError, ID: m.ID, Time: m.CreatedAt, Text: reason, Agent: agents.Signer(m.Message), turn: turn})
			}
		case m.Kind == store.KindForkReport:
			add(m, ThreadItem{
				Kind: ItemReport, ID: m.ID, Time: m.CreatedAt,
				Author: NewPerson(m.UserID, authorName(m)), Mine: m.UserID != "" && m.UserID == viewerID,
				HTML: Markdown(text(m.Content)), Report: reportSource(m.Fork, visible),
			})
		case m.Role == store.RoleUser:
			add(m, ThreadItem{
				Kind: ItemHuman, ID: m.ID, Time: m.CreatedAt,
				Author: NewPerson(m.UserID, authorName(m)), Mine: m.UserID != "" && m.UserID == viewerID,
				Text: text(m.Content),
			})
		case m.Role == store.RoleAssistant:
			signer := agents.Signer(m.Message)
			// A scheduled result is an item of its own, even after another
			// of the same agent.
			if agent != nil && (agent.Agent.ID != signer.ID || agent.turn != turn || store.IsScheduledResult(m.Key)) {
				closeAgent()
			}
			if agent == nil {
				agent = &ThreadItem{Kind: ItemAgent, Time: m.CreatedAt, Agent: signer, turn: turn,
					Running: states[turn] == turnRunning, Stopped: states[turn] == turnStopped}
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
			if turn == "" {
				// A message no turn wrote (a scheduled result) can be
				// answered: it is quoted as its item, once closed.
				agentPlain = append(agentPlain, m.ID)
				plain[m.ID] = m
			}
		}
		// Tool results are not shown: the tools appear on the answer.
	}
	closeAgent()

	for i := range items {
		if items[i].ForkID == 0 {
			items[i].ForkID = items[i].ID
		}
		turn := items[i].turn
		if turn == "" || i > 0 && items[i-1].turn == turn {
			continue // not a turn, or not its first item
		}
		anchor, _ := store.TurnAnchor(turn)
		if j, ok := at[anchor]; ok && j != i-1 {
			items[i].Quote = quoteOf(items[j], plain[anchor])
		}
	}
	return items
}

// FileLink is a file a turn published, as the thread lists it.
type FileLink struct {
	ID   string
	Name string
	Size string // « 12,4 Ko »
	// Via names the agent that published it when it is not the turn's: a
	// sub-agent the turn launched. Empty otherwise.
	Via string
}

// FileHref is where a file is downloaded.
func (f FileLink) Href() string { return "/files/" + f.ID }

// AttachFiles puts a session's files under the turns that published them:
// on the turn's answer (its last item, should it show as several), else on
// its error. A turn the thread does not show yet — it published from its
// first step, or from a sub-agent, before writing anything — gets an item
// of its own at the thread's end, before the questions, until it shows.
// A file a turn published twice, same name and same content (the model
// called again), shows once.
func AttachFiles(items []ThreadItem, files []store.File, agents AgentDirectory) []ThreadItem {
	if len(files) == 0 {
		return items
	}
	var order []string // turns, in the order of their first file
	byTurn := map[string][]store.File{}
	seen := map[[3]string]bool{}
	for _, f := range files {
		k := [3]string{f.TurnKey, f.Name, f.SHA256}
		if seen[k] {
			continue
		}
		seen[k] = true
		if _, ok := byTurn[f.TurnKey]; !ok {
			order = append(order, f.TurnKey)
		}
		byTurn[f.TurnKey] = append(byTurn[f.TurnKey], f)
	}
	agentAt, errorAt := map[string]int{}, map[string]int{}
	for i, it := range items {
		switch {
		case it.turn == "":
		case it.Kind == ItemAgent:
			agentAt[it.turn] = i
		case it.Kind == ItemError:
			errorAt[it.turn] = i
		}
	}
	var orphans []ThreadItem
	for _, turn := range order {
		links := fileLinks(byTurn[turn], agents)
		if i, ok := agentAt[turn]; ok {
			items[i].Files = links
		} else if i, ok := errorAt[turn]; ok {
			items[i].Files = links
		} else {
			first := byTurn[turn][0]
			orphans = append(orphans, ThreadItem{Kind: ItemFiles, Time: first.CreatedAt,
				Agent: agents.Signer(store.Message{AgentID: first.AgentID}), Files: links, turn: turn})
		}
	}
	if len(orphans) == 0 {
		return items
	}
	at := len(items)
	for at > 0 && items[at-1].Kind == ItemQuestion {
		at--
	}
	return slices.Concat(items[:at:at], orphans, items[at:])
}

func fileLinks(files []store.File, agents AgentDirectory) []FileLink {
	links := make([]FileLink, len(files))
	for i, f := range files {
		links[i] = FileLink{ID: f.ID, Name: f.Name, Size: FileSize(f.Size)}
		if f.AgentID != "" && f.AgentID != store.TurnParticipant(f.TurnKey) {
			links[i].Via = agents.Signer(store.Message{AgentID: f.AgentID}).Name
		}
	}
	return links
}

// FileSize is a size as the members read it: 820 o, 12,4 Ko, 3,1 Mo.
func FileSize(n int64) string {
	format := func(v float64, unit string) string {
		return strings.Replace(fmt.Sprintf("%.1f %s", v, unit), ".", ",", 1)
	}
	switch {
	case n < 1024:
		return fmt.Sprintf("%d o", n)
	case n < 1024*1024:
		return format(float64(n)/1024, "Ko")
	}
	return format(float64(n)/(1024*1024), "Mo")
}

// sameMinute reports whether a and b fall in the same minute of the clock:
// a turn that ended in the minute it started shows one time.
func sameMinute(a, b time.Time) bool {
	return a.Truncate(time.Minute).Equal(b.Truncate(time.Minute))
}

// quoteOf is how a turn's item recalls the message it answers, shown as the
// item it.
func quoteOf(it ThreadItem, m store.MessageWithID) *Quote {
	q := &Quote{Target: fmt.Sprintf("m%d", it.ID)}
	switch it.Kind {
	case ItemBrief:
		q.Target, q.Label = "brief", "en réponse au brief"
	case ItemReport:
		q.Label = "en réponse au rapport du fork « " + it.Report.Title + " »"
	case ItemAgent:
		q.Label, q.Text = "en réponse à "+it.Agent.Name, clipLine(text(m.Content), maxQuoteRunes)
	default:
		q.Label, q.Text = "en réponse à "+it.Author.Name, clipLine(text(m.Content), maxQuoteRunes)
	}
	return q
}

// clipLine is s on one line, its spaces collapsed, cut to n runes with an
// ellipsis.
func clipLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > n {
		return strings.TrimRight(string(r[:n-1]), " ") + "…"
	}
	return s
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
// after the last item it covers (ID up to the report's last message), before
// the questions waiting. The parent is linked when the viewer is a member of
// it. A fork that never reported is left as it is.
//
// The thread is chronological (BuildThread), a turn shown where it ended or
// at the end while it runs, and a report covers the messages up to an ID:
// a turn whose answer the report covers, which went on after it, still
// puts the mark after it, below the messages shown before it, reported or
// not; a turn still running, its answer covered, puts it at the end.
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

// LastMessageID is the last message of the thread to fork from: the highest
// fork point (ForkID) of its messages, answers and reports. The thread is
// chronological, but a turn still running shows at the end with the ID of
// its last text: its last item need not be its latest message.
func LastMessageID(items []ThreadItem) int64 {
	var last int64
	for _, it := range items {
		if it.Kind == ItemHuman || it.Kind == ItemAgent || it.Kind == ItemReport {
			last = max(last, it.ForkID)
		}
	}
	return last
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

// AgentsPanel is the rail's "Agents" panel: a row per participant of the
// session, the session's agent first, then the others in the order of
// their names.
type AgentsPanel struct {
	Rows []AgentRow
	// CanStop: the viewer may stop one of the turns running, at least
	// (the thread's Arrêter stops those).
	CanStop bool
	Error   string // why the viewer's last stop did not go
}

// AgentRow is a participant of the session, as the viewer sees it.
type AgentRow struct {
	// Participant is its name in the session: its row's element, its
	// actions' path.
	Participant string
	Agent       AgentInfo
	Working     bool
	Waiting     bool      // a question of its turn waits for a member's answer
	Author      string    // whom it answers: the author of its message; "" if not known
	Mine        bool      // it answers the viewer
	Since       time.Time // when its turn started; zero if not known
	Note        string    // what its turn waits for (a worker)
	Queued      int
	Background  []string
	// Turn is the turn shown: a stop names it, and stops no other.
	Turn     string
	CanStop  bool // the viewer may stop its turn (session.MayStop)
	CanClear bool // the viewer may drop its queue too (session.MayClear)
}

// Status says where the participant stands, in words: "Disponible",
// "Répond à Alice depuis 16:27", "Te répond depuis 16:27 · attend une
// réponse", "En cours" (at work, on a turn nothing described).
func (r AgentRow) Status() string {
	if !r.Working {
		if r.Queued > 0 {
			return "Passe au message suivant"
		}
		return "Disponible"
	}
	line := "Répond"
	switch {
	case r.Mine:
		line = "Te répond"
	case r.Author != "":
		line = "Répond à " + r.Author
	case r.Since.IsZero():
		line = "En cours" // at work, on a message nobody named
	}
	if !r.Since.IsZero() {
		line += " depuis " + clock(r.Since)
	}
	if r.Waiting {
		line += " · attend une réponse"
	}
	return line
}

// ClearConfirm is what Tout arrêter asks before it posts: it drops other
// members' messages too.
func (r AgentRow) ClearConfirm() string {
	what := "vider sa file"
	if r.Queued > 0 {
		what = "jeter " + pluralize(r.Queued, "message") + " de sa file"
	}
	return "Arrêter " + r.Agent.Name + " et " + what + ", ceux des autres membres compris ?"
}

// State is the row's dot: "waiting", "working" or "idle".
func (r AgentRow) State() Status {
	switch {
	case r.Waiting:
		return StatusWaiting
	case r.Working:
		return StatusWorking
	}
	return StatusIdle
}

// BuildAgents makes the Agents panel from where the session's participants
// stand (session.Participants), for the viewer: the session's agent always
// has its row, available if it has no participant. Each agent is named as
// it is now, else as its turn event named it. The viewer may stop a turn
// answering them, any if they created the session, and drop a queue only
// then (session.MayStop, session.MayClear).
func BuildAgents(ps []session.Participant, sess store.Session, viewerID string, agents AgentDirectory) AgentsPanel {
	var panel AgentsPanel
	row := func(p session.Participant) AgentRow {
		agent, ok := agents.ByID[p.AgentID]
		if !ok {
			agent = AgentInfo{ID: p.AgentID, Name: cmp.Or(p.Name, p.AgentID), Mention: p.AgentID}
		}
		r := AgentRow{
			Participant: p.Participant, Agent: agent, Working: p.Working, Waiting: p.Waiting,
			Author: p.UserName, Mine: p.UserID != "" && p.UserID == viewerID, Since: p.Since,
			Note: p.Note, Queued: p.Queued, Background: p.Background, Turn: p.Turn,
		}
		r.CanStop = p.Working && session.MayStop(&sess, p.UserID, viewerID)
		r.CanClear = (p.Working || p.Queued > 0) && session.MayClear(&sess, viewerID)
		panel.CanStop = panel.CanStop || r.CanStop
		return r
	}
	first := agents.Session.ID
	i := slices.IndexFunc(ps, func(p session.Participant) bool { return p.Participant == first })
	switch {
	case first == "":
	case i < 0:
		panel.Rows = append(panel.Rows, AgentRow{Participant: first, Agent: agents.Session})
	default:
		panel.Rows = append(panel.Rows, row(ps[i]))
	}
	for _, p := range ps {
		if p.Participant != first {
			panel.Rows = append(panel.Rows, row(p))
		}
	}
	return panel
}

// Working are the agents at work, as the panel shows them, for the
// thread's working line.
func (a AgentsPanel) Working() []WorkingAgent {
	var out []WorkingAgent
	for _, r := range a.Rows {
		if r.Working {
			out = append(out, WorkingAgent{Name: r.Agent.Name, Note: r.Note})
		}
	}
	return out
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
