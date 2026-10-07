package chat

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/victor/temporal-agent/conversation"
	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/store"
)

var t0 = time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

func sess(id, parent string, at int64, created time.Time) store.Session {
	return store.Session{SessionID: id, Title: id, ParentSessionID: parent, ForkedAtMessageID: at, CreatedAt: created}
}

func ids(nodes []*TreeNode) []string {
	var out []string
	for _, n := range nodes {
		out = append(out, n.Session.SessionID)
	}
	return out
}

func TestBuildTree(t *testing.T) {
	sessions := []store.Session{
		sess("old", "", 0, t0),
		sess("root", "", 0, t0.Add(time.Hour)),
		sess("b2", "root", 9, t0.Add(2*time.Hour)),
		sess("b1", "root", 3, t0.Add(3*time.Hour)), // forked earlier in the thread
		sess("leaf", "b1", 5, t0.Add(4*time.Hour)),
		sess("orphan", "hidden", 1, t0.Add(5*time.Hour)), // parent not visible
	}
	stats := map[string]store.SessionStats{
		"old":  {LastActivity: t0.Add(10 * time.Hour)}, // recently active
		"leaf": {LastActivity: t0.Add(11 * time.Hour)}, // makes its root the most recent
	}
	statuses := map[string]Status{"leaf": StatusWaiting}

	roots := BuildTree(sessions, stats, statuses, "leaf")
	if got := strings.Join(ids(roots), ","); got != "root,old,orphan" {
		t.Errorf("roots %s", got)
	}
	root := roots[0]
	// Children in the order of the messages they forked from.
	if got := strings.Join(ids(root.Children), ","); got != "b1,b2" {
		t.Errorf("children %s", got)
	}
	leaf := Find(roots, "leaf")
	if leaf == nil || leaf.Depth != 2 || !leaf.Current || leaf.Status != StatusWaiting || Root(leaf) != root {
		t.Errorf("leaf %+v", leaf)
	}
	if got := strings.Join(ids(Path(leaf)), ","); got != "root,b1,leaf" {
		t.Errorf("path %s", got)
	}
	if o := Find(roots, "orphan"); !o.Orphan || o.Depth != 0 {
		t.Errorf("orphan %+v", o)
	}
	if Find(roots, "old").Status != StatusIdle {
		t.Error("no status means idle")
	}
	if Count(root) != 4 {
		t.Errorf("count %d", Count(root))
	}
}

func j(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestBuildThread(t *testing.T) {
	msgs := []store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: j("**brief**")}},
		{ID: 2, Message: store.Message{Role: store.RoleUser, Content: j("hello"), UserID: "u-me", Author: "Victor F"}},
		{ID: 3, Message: store.Message{Role: store.RoleUser, Content: j("@agent go"), UserID: "u-bob", Author: "Bob"}},
		// One agent turn over four messages.
		{ID: 4, Message: store.Message{Role: store.RoleAssistant, Content: j("Let me look."), ToolCalls: []store.ToolCall{{ID: "t1", Name: "read_file"}}}},
		{ID: 5, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "data"}}},
		{ID: 6, Message: store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t2", Name: "read_file"}, {ID: "t3", Name: "grep"}}}},
		{ID: 7, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t2"}}},
		{ID: 8, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t3"}}},
		{ID: 9, Message: store.Message{Role: store.RoleAssistant, Content: j("Done.")}},
		{ID: 10, Message: store.Message{Role: store.RoleAssistant, Kind: store.KindTurnEnd, Content: j("call LLM: boom")}},
	}
	forks := map[int64][]ForkLink{9: {{SessionID: "f1", Title: "Fork"}}}
	questions := []Question{{WorkflowID: "s1:p:default:m3:tool:ask_user:1-0", Text: "Which one?"}}

	jarvis := AgentInfo{ID: "default", Name: "Jarvis", Mention: "jarvis"}
	items := BuildThread(msgs, "u-me", forks, questions, AgentDirectory{Session: jarvis})
	var kinds []string
	for _, it := range items {
		kinds = append(kinds, it.Kind)
	}
	if got := strings.Join(kinds, ","); got != "brief,human,human,agent,error,question" {
		t.Fatalf("kinds %s", got)
	}
	if !strings.Contains(string(items[0].HTML), "<strong>brief</strong>") {
		t.Errorf("brief %s", items[0].HTML)
	}
	if !items[1].Mine || items[2].Mine || items[2].Author.Initials != "BO" || items[1].Author.Initials != "VF" {
		t.Errorf("humans %+v / %+v", items[1], items[2])
	}
	agent := items[3]
	// Its text, its tools once each in order, and its last message to fork from.
	if strings.Join(agent.Tools, ",") != "read_file,grep" || agent.ID != 9 ||
		!strings.Contains(string(agent.HTML), "Let me look.") || !strings.Contains(string(agent.HTML), "Done.") {
		t.Errorf("agent %+v", agent)
	}
	if agent.Agent != jarvis || items[4].Agent != jarvis {
		t.Errorf("signed %+v / %+v, want the session's agent for messages no agent signed", agent.Agent, items[4].Agent)
	}
	if len(agent.Forks) != 1 || agent.Forks[0].SessionID != "f1" {
		t.Errorf("forks %+v", agent.Forks)
	}
	// A failure is an item of its own, not more of the agent's answer.
	if items[4].Text != "call LLM: boom" || strings.Contains(string(agent.HTML), "boom") {
		t.Errorf("error %+v", items[4])
	}
	if items[5].WorkflowID != "s1:p:default:m3:tool:ask_user:1-0" {
		t.Errorf("question %+v", items[5])
	}
}

// A turn's end with no error is shown nowhere: it only closes the turn, so
// the same agent's next turn is an item of its own.
func TestBuildThread_HidesATurnEndWithoutError(t *testing.T) {
	msgs := []store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Content: j("M1"), UserID: "u-me"}},
		{ID: 2, Message: store.Message{Role: store.RoleUser, Content: j("M2"), UserID: "u-me"}},
		{ID: 3, Message: store.Message{Role: store.RoleAssistant, Content: j("R1"), AgentID: "default"}},
		{ID: 4, Message: store.TurnEnd("default", "")},
		{ID: 5, Message: store.Message{Role: store.RoleAssistant, Content: j("R2"), AgentID: "default"}},
		{ID: 6, Message: store.TurnEnd("default", "")},
	}
	items := BuildThread(msgs, "u-me", nil, nil, AgentDirectory{Session: AgentInfo{ID: "default"}})
	var got []string
	for _, it := range items {
		got = append(got, fmt.Sprint(it.Kind, it.ID))
	}
	if want := "human1 human2 agent3 agent5"; strings.Join(got, " ") != want {
		t.Errorf("items %v, want %s", got, want)
	}
}

// Several agents answer one after another: an item per agent, each signed by
// its agent as it is now, or by the name it had once it is gone.
func TestBuildThread_SignsEachAgent(t *testing.T) {
	jarvis := AgentInfo{ID: "default", Name: "Jarvis", Mention: "jarvis"}
	smith := AgentInfo{ID: "smith", Name: "Agent Smith", Mention: "agentSmith"}
	msgs := []store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Content: j("@jarvis résume, @agentSmith juge"), UserID: "u-me", Author: "Victor"}},
		{ID: 2, Message: store.Message{Role: store.RoleAssistant, AgentID: "default", Author: "Old Jarvis", ToolCalls: []store.ToolCall{{ID: "t1", Name: "web_search"}}}},
		{ID: 3, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}},
		{ID: 4, Message: store.Message{Role: store.RoleAssistant, Content: j("Résumé."), AgentID: "default", Author: "Old Jarvis"}},
		{ID: 5, Message: store.Message{Role: store.RoleAssistant, Content: j("Utile."), AgentID: "smith", Author: "Smith"}},
		{ID: 6, Message: store.Message{Role: store.RoleAssistant, Content: j("Moi aussi."), AgentID: "gone", Author: "Ancien"}},
		{ID: 7, Message: store.Message{Role: store.RoleAssistant, Content: j("Sans nom."), AgentID: "nameless"}},
		{ID: 8, Message: store.Message{Role: store.RoleAssistant, Kind: store.KindTurnEnd, AgentID: "smith", Content: j("boom")}},
	}
	items := BuildThread(msgs, "u-me", nil, nil, AgentDirectory{ByID: map[string]AgentInfo{"default": jarvis, "smith": smith}, Session: jarvis})

	want := []struct {
		kind string
		id   int64
		who  AgentInfo
	}{
		{ItemHuman, 1, AgentInfo{}},
		{ItemAgent, 4, jarvis}, // its current name, not the one stored
		{ItemAgent, 5, smith},
		{ItemAgent, 6, AgentInfo{ID: "gone", Name: "Ancien"}},
		{ItemAgent, 7, AgentInfo{ID: "nameless", Name: "nameless"}},
		{ItemError, 8, smith},
	}
	if len(items) != len(want) {
		t.Fatalf("%d items, want %d: %+v", len(items), len(want), items)
	}
	for i, w := range want {
		if it := items[i]; it.Kind != w.kind || it.ID != w.id || it.Agent != w.who {
			t.Errorf("item %d: %s #%d by %+v, want %s #%d by %+v", i, it.Kind, it.ID, it.Agent, w.kind, w.id, w.who)
		}
	}
	if strings.Join(items[1].Tools, ",") != "web_search" || strings.Contains(string(items[2].HTML), "Résumé") {
		t.Errorf("jarvis's item %+v, smith's %s: each keeps its own", items[1], items[2].HTML)
	}
}

// parallel is two participants answering at once, as stored: Jarvis
// answers Alice's message 1 with a tool, Bob writes message 4 meanwhile,
// Smith answers it and is done before Jarvis is. With done, Jarvis's
// answer and its end follow.
func parallel(done bool) []store.MessageWithID {
	jarvis := func(id int64, i string, m store.Message) store.MessageWithID {
		m.AgentID = "default"
		return store.MessageWithID{ID: id, Key: "m1.default:" + i, Message: m}
	}
	smith := func(id int64, i string, m store.Message) store.MessageWithID {
		m.AgentID = "smith"
		return store.MessageWithID{ID: id, Key: "m4.smith:" + i, Message: m}
	}
	msgs := []store.MessageWithID{
		{ID: 1, Key: "msg:a", Message: store.Message{Role: store.RoleUser, Content: j("Jarvis, cherche"), UserID: "u-alice", Author: "Alice"}},
		jarvis(2, "0", store.Message{Role: store.RoleAssistant, Content: j("Je cherche."), ToolCalls: []store.ToolCall{{ID: "t1", Name: "web_fetch"}}}),
		jarvis(3, "1", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "page"}}),
		{ID: 4, Key: "msg:b", Message: store.Message{Role: store.RoleUser, Content: j("@smith juge"), UserID: "u-bob", Author: "Bob"}},
		smith(5, "0", store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t2", Name: "grep"}}}),
		smith(6, "1", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t2"}}),
		smith(7, "2", store.Message{Role: store.RoleAssistant, Content: j("Verdict.")}),
		{ID: 8, Key: "m4.smith:end", Message: store.TurnEnd("smith", "")},
	}
	if done {
		msgs = append(msgs,
			jarvis(9, "2", store.Message{Role: store.RoleAssistant, Content: j("Trouvé.")}),
			store.MessageWithID{ID: 10, Key: "m1.default:end", Message: store.TurnEnd("default", "")})
	}
	return msgs
}

// hist writes a session's history as the store numbers it: each message the
// next ID, a minute after the one before.
type hist struct {
	msgs []store.MessageWithID
	idx  map[string]int
}

func (h *hist) add(key string, m store.Message) int64 {
	id := int64(len(h.msgs) + 1)
	h.msgs = append(h.msgs, store.MessageWithID{ID: id, Key: key, CreatedAt: t0.Add(time.Duration(id) * time.Minute), Message: m})
	return id
}

// human is a member's message.
func (h *hist) human(who, text string) int64 {
	return h.add(fmt.Sprintf("msg:%d", len(h.msgs)+1), store.Message{Role: store.RoleUser, Content: j(text), UserID: "u-" + who, Author: who})
}

// say is a turn's message: its text, and the tools it calls, each followed
// by its result.
func (h *hist) say(turn, text string, tools ...string) int64 {
	if h.idx == nil {
		h.idx = map[string]int{}
	}
	key := func() string {
		i := h.idx[turn]
		h.idx[turn]++
		return fmt.Sprintf("%s:%d", turn, i)
	}
	m := store.Message{Role: store.RoleAssistant, Content: j(text), AgentID: store.TurnParticipant(turn)}
	for i, name := range tools {
		m.ToolCalls = append(m.ToolCalls, store.ToolCall{ID: fmt.Sprintf("c%d-%d", len(h.msgs), i), Name: name})
	}
	id := h.add(key(), m)
	for _, tc := range m.ToolCalls {
		h.add(key(), store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: tc.ID}})
	}
	return id
}

// end is a turn's end, failed for reason ("": done).
func (h *hist) end(turn, reason string) int64 {
	return h.add(store.TurnEndKey(turn), store.TurnEnd(store.TurnParticipant(turn), reason))
}

// shape is a thread in short: each item's kind and ID, "…" while it runs,
// "↩" and the element it quotes.
func shape(items []ThreadItem) string {
	var out []string
	for _, it := range items {
		s := fmt.Sprint(it.Kind, it.ID)
		if it.Running {
			s += "…"
		}
		if it.Quote != nil {
			s += "↩" + it.Quote.Target
		}
		out = append(out, s)
	}
	return strings.Join(out, " ")
}

var (
	jarvisInfo   = AgentInfo{ID: "default", Name: "Jarvis", Mention: "jarvis"}
	reviewerInfo = AgentInfo{ID: "reviewer", Name: "Reviewer", Mention: "reviewer"}
	directory    = AgentDirectory{ByID: map[string]AgentInfo{"default": jarvisInfo, "reviewer": reviewerInfo, "smith": smith}, Session: jarvisInfo}
)

// Two participants answer at once: each answer shows whole where it ended,
// signed by its agent, a turn still running at the end; one shown away from
// the message it answers quotes it. In the order of the IDs, Jarvis's
// would be cut by Bob's message and Smith's answer.
func TestBuildThread_TwoParticipantsInParallel(t *testing.T) {
	jarvis := AgentInfo{ID: "default", Name: "Jarvis", Mention: "jarvis"}
	smith := AgentInfo{ID: "smith", Name: "Agent Smith", Mention: "smith"}
	dir := AgentDirectory{ByID: map[string]AgentInfo{"default": jarvis, "smith": smith}, Session: jarvis}
	for name, c := range map[string]struct {
		done  bool
		shape string
		text  string // Jarvis's answer
		last  int64
	}{
		"jarvis running": {false, "human1 human4 agent7 agent2…↩m1", "Je cherche.", 8},
		"both done":      {true, "human1 human4 agent7 agent9↩m1", "Je cherche.\n\nTrouvé.", 10},
	} {
		t.Run(name, func(t *testing.T) {
			items := BuildThread(parallel(c.done), "u-alice", nil, nil, dir)
			if got := shape(items); got != c.shape {
				t.Fatalf("thread %s, want %s", got, c.shape)
			}
			s, j := items[2], items[3]
			if s.Agent != smith || strings.Join(s.Tools, ",") != "grep" || string(s.HTML) != string(Markdown("Verdict.")) {
				t.Errorf("smith's answer %+v", s)
			}
			if j.Agent != jarvis || strings.Join(j.Tools, ",") != "web_fetch" || string(j.HTML) != string(Markdown(c.text)) {
				t.Errorf("jarvis's answer %+v, want %q whole", j, c.text)
			}
			if *j.Quote != (Quote{Target: "m1", Label: "en réponse à Alice", Text: "Jarvis, cherche"}) {
				t.Errorf("quote %+v", *j.Quote)
			}
			if got := LastMessageID(items); got != c.last {
				t.Errorf("last message %d, want %d: the highest fork point, not the last item's", got, c.last)
			}
		})
	}
}

// The user's case: Victor asks the reviewer for a long review, then Jarvis
// the time. Jarvis answers first, right under its question; the review
// comes after it, when it ended, and quotes what it answers.
func TestBuildThread_Chronological(t *testing.T) {
	var h hist
	h.human("Victor", "@reviewer relis la PR 42 en entier") // 1
	h.say("m1.reviewer", "", "read_file")                   // 2, 3
	h.human("Victor", "@jarvis quelle heure est-il ?")      // 4
	h.say("m4.default", "Midi.")                            // 5
	h.end("m4.default", "")                                 // 6
	h.say("m1.reviewer", "La PR est bonne.")                // 7
	h.end("m1.reviewer", "")                                // 8

	items := BuildThread(h.msgs, "u-Victor", nil, nil, directory)
	if got, want := shape(items), "human1 human4 agent5 agent7↩m1"; got != want {
		t.Fatalf("thread %s, want %s", got, want)
	}
	review := items[3]
	if review.Agent != reviewerInfo || strings.Join(review.Tools, ",") != "read_file" ||
		*review.Quote != (Quote{Target: "m1", Label: "en réponse à Victor", Text: "@reviewer relis la PR 42 en entier"}) {
		t.Errorf("review %+v, quote %+v", review, review.Quote)
	}
	// Each shows when it started, then when it ended.
	if !review.Time.Equal(t0.Add(2*time.Minute)) || !review.End.Equal(t0.Add(8*time.Minute)) ||
		!items[2].Time.Equal(t0.Add(5*time.Minute)) || !items[2].End.Equal(t0.Add(6*time.Minute)) {
		t.Errorf("jarvis %v → %v, review %v → %v: want start → end", items[2].Time, items[2].End, review.Time, review.End)
	}
	if LastMessageID(items) != 8 {
		t.Errorf("last message %d, want the review's end", LastMessageID(items))
	}
}

// Forking from a block takes what the thread shows above it: a turn that
// ended forks up to its end, not to its last text. The reviewer said « Je
// lis. » first, then worked while Victor asked Jarvis: the review shows
// last, and a fork from it must keep Jarvis's question and answer.
func TestBuildThread_ForkFromABlockTakesWhatIsAbove(t *testing.T) {
	var h hist
	h.human("Victor", "@reviewer relis")         // 1
	h.say("m1.reviewer", "Je lis.", "read_file") // 2, 3
	h.human("Victor", "@jarvis l'heure ?")       // 4
	h.say("m4.default", "Midi.")                 // 5
	h.end("m4.default", "")                      // 6
	h.say("m1.reviewer", "", "read_file")        // 7, 8
	end := h.end("m1.reviewer", "")              // 9
	items := BuildThread(h.msgs, "u-Victor", nil, nil, directory)
	if got, want := shape(items), "human1 human4 agent5 agent2↩m1"; got != want {
		t.Fatalf("thread %s, want %s", got, want)
	}
	review := items[3]
	if review.ID != 2 || review.ForkID != end {
		t.Fatalf("review: element m%d, forks from %d; want m2 (its last text), %d (its end)", review.ID, review.ForkID, end)
	}
	// What session.Fork accepts (forkable): a user's or assistant's
	// message, not a tool result. It takes every message up to it.
	fork := h.msgs[review.ForkID-1]
	if fork.ID != review.ForkID || fork.Role != store.RoleAssistant || fork.ToolResult != nil {
		t.Errorf("fork point %+v: not one a fork accepts", fork)
	}
	for _, it := range items {
		if it.ForkID > review.ForkID {
			t.Errorf("%s: %d shown above the review, left out of its fork", shape([]ThreadItem{it}), it.ForkID)
		}
	}
	if LastMessageID(items) != end {
		t.Errorf("« Forker un fil » from %d, want %d", LastMessageID(items), end)
	}
	// The element keeps its ID; the fork goes up to the end.
	p := testPage("thread")
	p.Thread = items
	page := render(t, "thread-inner", p)
	if !strings.Contains(page, `id="m2" data-fork="m9"`) || !strings.Contains(page, `name="message_id" value="9"`) ||
		strings.Contains(page, `name="message_id" value="2"`) {
		t.Errorf("fork menu: %s", page)
	}
	// Jarvis's ended too: its fork point is its end; a member's, the message.
	if items[2].ForkID != 6 || items[0].ForkID != 1 {
		t.Errorf("fork points %d, %d", items[2].ForkID, items[0].ForkID)
	}
}

// A block's time is its start, then → its end once it ended in another
// minute, « → en cours » while it runs, « → interrompu » if it never ends:
// it completes, never jumps.
func TestRender_BlockTime(t *testing.T) {
	at := func(min, sec int) time.Time {
		return t0.Add(time.Duration(min)*time.Minute + time.Duration(sec)*time.Second)
	}
	human := func(id int64, at time.Time) store.MessageWithID {
		return store.MessageWithID{ID: id, Key: fmt.Sprintf("msg:%d", id), CreatedAt: at, Message: store.Message{Role: store.RoleUser, Content: j("?"), UserID: "u-v", Author: "V"}}
	}
	turn := func(id int64, key string, at time.Time, agent string) store.MessageWithID {
		m := store.TurnEnd(agent, "")
		if !strings.HasSuffix(key, ":end") {
			m = store.Message{Role: store.RoleAssistant, Content: j("!"), AgentID: agent}
		}
		return store.MessageWithID{ID: id, Key: key, CreatedAt: at, Message: m}
	}
	msgs := []store.MessageWithID{
		human(1, at(0, 0)),
		turn(2, "m1.default:0", at(0, 10), "default"),
		turn(3, "m1.default:end", at(0, 50), "default"), // the same minute
		human(4, at(1, 0)),
		turn(5, "m4.default:0", at(1, 10), "default"),
		turn(6, "m4.default:end", at(11, 0), "default"),
		human(7, at(12, 0)),
		turn(8, "m7.reviewer:0", at(12, 10), "reviewer"), // never ends
		human(9, at(13, 0)),
		turn(10, "m9.reviewer:0", at(13, 10), "reviewer"), // runs
	}
	items := BuildThread(msgs, "u-v", nil, nil, directory)
	if got, want := shape(items), "human1 agent2 human4 agent5 human7 agent8 human9 agent10…"; got != want {
		t.Fatalf("thread %s, want %s", got, want)
	}
	if !items[1].End.IsZero() || !items[3].End.Equal(at(11, 0)) || !items[5].Stopped || items[5].Running {
		t.Errorf("quick %+v, long %+v, dead %+v", items[1], items[3], items[5])
	}
	p := testPage("thread")
	p.Thread = items
	page := render(t, "thread-inner", p)
	var got []string
	for _, w := range regexp.MustCompile(`<span class="when">([^<]*)</span>`).FindAllStringSubmatch(page, -1) {
		got = append(got, w[1])
	}
	want := []string{
		clock(at(0, 0)), clock(at(0, 10)),
		clock(at(1, 0)), clock(at(1, 10)) + " → " + clock(at(11, 0)),
		clock(at(12, 0)), clock(at(12, 10)) + " → interrompu",
		clock(at(13, 0)), clock(at(13, 10)) + " → en cours",
	}
	if !slices.Equal(got, want) {
		t.Errorf("times %q, want %q", got, want)
	}
}

// A turn still running shows at the end, after a message written since,
// marked running; once ended, where it ended, before what came after.
func TestBuildThread_RunningTurnAtTheEnd(t *testing.T) {
	var h hist
	h.human("Victor", "@reviewer relis") // 1
	h.say("m1.reviewer", "Je lis.", "read_file")
	h.human("Victor", "@jarvis l'heure ?") // 4
	h.say("m4.default", "Midi.")
	h.end("m4.default", "")
	h.human("Victor", "merci") // 7

	items := BuildThread(h.msgs, "u-Victor", nil, []Question{{WorkflowID: "q", Text: "Quelle PR ?"}}, directory)
	if got, want := shape(items), "human1 human4 agent5 human7 agent2…↩m1 question0"; got != want {
		t.Errorf("running: %s, want %s, before the questions", got, want)
	}
	if !items[4].Time.Equal(t0.Add(2 * time.Minute)) {
		t.Errorf("a running answer shows when it started: %v", items[4].Time)
	}

	h.say("m1.reviewer", "Bonne.") // 8
	h.end("m1.reviewer", "")       // 9
	h.human("Victor", "super")     // 10
	if got, want := shape(BuildThread(h.msgs, "u-Victor", nil, nil, directory)), "human1 human4 agent5 human7 agent8↩m1 human10"; got != want {
		t.Errorf("ended: %s, want %s", got, want)
	}
}

// A turn without an end that its participant followed with another will
// never end (its participant was stopped from outside): it shows where it
// stopped, interrupted, not as running.
func TestBuildThread_StoppedTurnDoesNotRun(t *testing.T) {
	var h hist
	h.human("Victor", "un")
	h.say("m1.default", "Je commence.")
	h.human("Victor", "deux")
	h.say("m3.default", "Fait.")
	h.end("m3.default", "")
	items := BuildThread(h.msgs, "u-Victor", nil, nil, directory)
	if got, want := shape(items), "human1 agent2 human3 agent4"; got != want {
		t.Errorf("thread %s, want %s", got, want)
	}
	if !items[1].Stopped || items[1].Running || items[3].Stopped {
		t.Errorf("dead %+v, next %+v", items[1], items[3])
	}
	p := testPage("thread")
	p.Thread = items
	if page := render(t, "thread-inner", p); strings.Count(page, "→ interrompu") != 1 || strings.Contains(page, "en cours") {
		t.Errorf("the dead turn not shown interrupted: %s", page)
	}
}

// An end alone is no proof its participant went on: a relay that failed
// writes the end of the turn it could not start (as would a queue cleared,
// or a turn refused by its check). Smith, still on message 1, stays running
// though message 5's relay to it failed.
func TestBuildThread_AnEndAloneDoesNotStopATurn(t *testing.T) {
	var h hist
	h.human("Victor", "@jarvis @smith un")              // 1
	h.say("m1.default", "Oui.")                         // 2
	h.end("m1.default", "")                             // 3
	h.say("m1.smith", "Je regarde.", "grep")            // 4, 5
	h.human("Victor", "@jarvis @smith deux")            // 6
	h.say("m6.default", "Toujours oui.")                // 7
	h.end("m6.default", "")                             // 8
	h.end("m6.smith", "le relais vers @smith a échoué") // 9
	items := BuildThread(h.msgs, "u-Victor", nil, nil, directory)
	if got, want := shape(items), "human1 agent2 human6 agent7 error9↩m6 agent4…↩m1"; got != want {
		t.Fatalf("thread %s, want %s", got, want)
	}
	if smith := items[5]; smith.Stopped || !smith.Running {
		t.Errorf("smith %+v: still running", smith)
	}
	p := testPage("thread")
	p.Thread = items
	if page := render(t, "thread-inner", p); !strings.Contains(page, "→ en cours") || strings.Contains(page, "interrompu") {
		t.Errorf("smith not shown running: %s", page)
	}
}

// A relay: each answer to one message quotes it when it does not follow it.
func TestBuildThread_RelayQuotesItsQuestion(t *testing.T) {
	var h hist
	h.human("Victor", "@jarvis @smith votre avis ?")
	h.say("m1.default", "Oui.")
	h.end("m1.default", "")
	h.say("m1.smith", "Non.")
	h.end("m1.smith", "")
	if got, want := shape(BuildThread(h.msgs, "u-Victor", nil, nil, directory)), "human1 agent2 agent4↩m1"; got != want {
		t.Errorf("relay: %s, want %s", got, want)
	}

	// Bob writes before either answers: both quote the question.
	h = hist{}
	h.human("Victor", "@jarvis @smith votre avis ?")
	h.human("Bob", "je reviens")
	h.say("m1.default", "Oui.")
	h.end("m1.default", "")
	h.say("m1.smith", "Non.")
	h.end("m1.smith", "")
	items := BuildThread(h.msgs, "u-Victor", nil, nil, directory)
	if got, want := shape(items), "human1 human2 agent3↩m1 agent5↩m1"; got != want {
		t.Errorf("relay after another message: %s, want %s", got, want)
	}
	if *items[2].Quote != *items[3].Quote {
		t.Errorf("quotes %+v, %+v: the same question", *items[2].Quote, *items[3].Quote)
	}
}

// A quote is the start of the question on one line; the template escapes
// it.
func TestBuildThread_QuoteIsClippedAndEscaped(t *testing.T) {
	var h hist
	h.human("Alice", "<script>alert(1)</script>\n\n"+strings.Repeat("très long ", 30))
	h.say("m1.reviewer", "", "read_file")
	h.human("Bob", "@jarvis salut")
	h.say("m4.default", "Salut.")
	h.end("m4.default", "")
	h.say("m1.reviewer", "Lu.")
	h.end("m1.reviewer", "")
	items := BuildThread(h.msgs, "u-Bob", nil, nil, directory)
	q := items[3].Quote
	if q == nil {
		t.Fatalf("no quote in %s", shape(items))
	}
	if n := utf8.RuneCountInString(q.Text); n != maxQuoteRunes || !strings.HasSuffix(q.Text, "…") ||
		!strings.HasPrefix(q.Text, "<script>alert(1)</script> très long très") || strings.Contains(q.Text, "\n") {
		t.Errorf("quote %q (%d runes)", q.Text, n)
	}

	p := testPage("thread")
	p.Thread = items
	page := render(t, "thread-inner", p)
	if strings.Contains(page, "<script>alert") || !strings.Contains(page, `<a class="quote" href="#m1"><span class="quote-to">↩ en réponse à Alice</span> : <span class="quote-text">&lt;script&gt;alert(1)&lt;/script&gt; très long`) {
		t.Errorf("quote rendered as %s", page)
	}
	// Jarvis follows Bob's message: no quote.
	if strings.Count(page, `class="quote"`) != 1 {
		t.Errorf("%d quotes, want 1", strings.Count(page, `class="quote"`))
	}
}

// A failed turn shows its error where it ended, quoting its question when
// it is away from it; with an answer, the answer quotes, not the error.
func TestBuildThread_FailedTurnQuotes(t *testing.T) {
	var h hist
	h.human("Victor", "@reviewer relis")
	h.human("Bob", "@jarvis salut")
	h.say("m2.default", "Salut.")
	h.end("m2.default", "")
	h.end("m1.reviewer", "call LLM: boom")
	items := BuildThread(h.msgs, "u-Victor", nil, nil, directory)
	if got, want := shape(items), "human1 human2 agent3 error5↩m1"; got != want {
		t.Fatalf("thread %s, want %s", got, want)
	}
	if items[3].Agent != reviewerInfo || items[3].Text != "call LLM: boom" {
		t.Errorf("error %+v", items[3])
	}
	p := testPage("thread")
	p.Thread = items
	if page := render(t, "thread-inner", p); !strings.Contains(page, `↩ en réponse à Victor</span> : <span class="quote-text">@reviewer relis</span>`) {
		t.Errorf("error's quote not rendered: %s", page)
	}

	h = hist{}
	h.human("Victor", "@reviewer relis")
	h.say("m1.reviewer", "", "read_file")
	h.human("Bob", "@jarvis salut")
	h.say("m4.default", "Salut.")
	h.end("m4.default", "")
	h.end("m1.reviewer", "call LLM: boom")
	if got, want := shape(BuildThread(h.msgs, "u-Victor", nil, nil, directory)), "human1 human4 agent5 agent2↩m1 error7"; got != want {
		t.Errorf("thread %s, want %s", got, want)
	}
}

// What a quote names: a member, the brief, a fork's report, a scheduled
// result's agent.
func TestBuildThread_QuotesWhatItAnswers(t *testing.T) {
	for name, c := range map[string]struct {
		first store.MessageWithID
		want  Quote
	}{
		"brief": {store.MessageWithID{Key: store.ForkSummaryKey, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: j("Le brief.")}},
			Quote{Target: "brief", Label: "en réponse au brief"}},
		"report": {store.MessageWithID{Key: "report:f:0-9", Message: store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: j("## Fait"), UserID: "u-Bob", Author: "Bob",
			Fork: &store.ForkRef{SessionID: "f", Title: "Export CSV"}}},
			Quote{Target: "m1", Label: "en réponse au rapport du fork « Export CSV »"}},
		"scheduled": {store.MessageWithID{Key: "sched:t:1", Message: store.Message{Role: store.RoleAssistant, Content: j("Rappel : réunion"), AgentID: "default"}},
			Quote{Target: "m1", Label: "en réponse à Jarvis", Text: "Rappel : réunion"}},
	} {
		t.Run(name, func(t *testing.T) {
			var h hist
			h.add(c.first.Key, c.first.Message)
			h.human("Bob", "rien pour l'agent")
			h.say("m1.default", "Vu.")
			h.end("m1.default", "")
			items := BuildThread(h.msgs, "u-Bob", nil, nil, directory)
			if got := shape(items); got != fmt.Sprintf("%s1 human2 agent3↩%s", items[0].Kind, c.want.Target) {
				t.Fatalf("thread %s", got)
			}
			if *items[2].Quote != c.want {
				t.Errorf("quote %+v, want %+v", *items[2].Quote, c.want)
			}
		})
	}
}

// When each message is written once the answers to the one before have
// ended, the thread is as it was in the order of the anchors, with no
// quote: random histories of members' messages, answers by one agent or
// another, with tools, errors, the brief, reports, scheduled results, the
// last answer still running or not. Which turns run is read from the
// history (a turn without an end), not from the code under test.
//
// Not covered, being no longer one at a time: a relay (several agents
// answering one message), an aside (/btw), a scheduled result stored while
// a turn runs.
func TestBuildThread_OneAtATimeAsBefore(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 42))
	agents := []string{"default", "smith", "reviewer"}
	n := 3000
	if testing.Short() {
		n = 300
	}
	for i := 0; i < n; i++ {
		var h hist
		if rng.IntN(4) == 0 {
			h.add(store.ForkSummaryKey, store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: j("brief")})
		}
		rounds := 1 + rng.IntN(6)
		unfinished := false // the last turn wrote and has no end
		for r := 0; r < rounds; r++ {
			switch rng.IntN(8) {
			case 0:
				h.add(fmt.Sprintf("sched:t:%d", r), store.Message{Role: store.RoleAssistant, Content: j("Rappel"), AgentID: "default"})
			case 1:
				h.add(fmt.Sprintf("report:f:0-%d", r), store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: j("Fait"), UserID: "u-Bob", Author: "Bob"})
			}
			q := h.human([]string{"Victor", "Bob"}[rng.IntN(2)], "question")
			if rng.IntN(5) == 0 {
				continue // no agent called
			}
			turn := store.TurnKey(q, agents[rng.IntN(len(agents))])
			said := rng.IntN(4)
			for s := said; s > 0; s-- {
				var tools []string
				if rng.IntN(2) == 0 {
					tools = []string{"grep", "read_file"}[:1+rng.IntN(2)]
				}
				h.say(turn, []string{"", "Réponse."}[rng.IntN(2)], tools...)
			}
			if r == rounds-1 && rng.IntN(3) == 0 {
				unfinished = said > 0
				break // still running
			}
			h.end(turn, []string{"", "", "", "boom"}[rng.IntN(4)])
		}

		ends := store.TurnEndIDs(h.msgs)
		states := map[string]turnState{}
		for _, m := range h.msgs {
			if turn, ok := store.TurnOf(m.Key); ok {
				if _, ended := ends[turn]; !ended {
					states[turn] = turnRunning
				}
			}
		}
		got := BuildThread(h.msgs, "u-Victor", nil, nil, directory)
		want := threadItems(conversation.Order(h.msgs), states, "u-Victor", map[string]bool{}, directory)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("history %d: thread %s, want %s as in the order of the anchors", i, shape(got), shape(want))
		}
		for k, it := range got {
			if it.Quote != nil {
				t.Fatalf("history %d: %s, a quote though each answer follows its question", i, shape(got))
			}
			if last := k == len(got)-1; it.Running != (last && unfinished) || it.Stopped {
				t.Fatalf("history %d (unfinished %v): %s, item %d running %v, stopped %v", i, unfinished, shape(got), k, it.Running, it.Stopped)
			}
		}
	}
}

// A turn is an item of its own, even next to an answer of the same agent
// that is not of it: a scheduled task's result stored while the turn runs
// shows before it, and the turn quotes its question.
func TestBuildThread_AnItemPerTurn(t *testing.T) {
	msgs := []store.MessageWithID{
		{ID: 1, Key: "msg:a", Message: store.Message{Role: store.RoleUser, Content: j("M1"), UserID: "u-me"}},
		{ID: 2, Key: "m1.default:0", Message: store.Message{Role: store.RoleAssistant, Content: j("R1"), AgentID: "default"}},
		{ID: 3, Key: "sched:t1:1", Message: store.Message{Role: store.RoleAssistant, Content: j("Rappel"), AgentID: "default"}},
		{ID: 4, Key: "m1.default:1", Message: store.Message{Role: store.RoleAssistant, Content: j("R1 bis"), AgentID: "default"}},
	}
	items := BuildThread(msgs, "u-me", nil, nil, AgentDirectory{Session: AgentInfo{ID: "default"}})
	if got, want := shape(items), "human1 agent3 agent4…↩m1"; got != want {
		t.Errorf("items %s, want %s: the task's result, then the turn whole", got, want)
	}
}

// Two scheduled results in a row are two items: a turn answering the first
// quotes it, not the second.
func TestBuildThread_ScheduledResultsApart(t *testing.T) {
	var h hist
	h.add("sched:t1:1", store.Message{Role: store.RoleAssistant, Content: j("Rappel 1"), AgentID: "default"})
	h.add("sched:t2:1", store.Message{Role: store.RoleAssistant, Content: j("Rappel 2"), AgentID: "default"})
	h.say("m1.default", "Vu le premier.")
	h.end("m1.default", "")
	items := BuildThread(h.msgs, "u-me", nil, nil, directory)
	if got, want := shape(items), "agent1 agent2 agent3↩m1"; got != want {
		t.Fatalf("items %s, want %s", got, want)
	}
	if q := items[2].Quote; q.Text != "Rappel 1" {
		t.Errorf("quote %+v", q)
	}
}

// The agent's text comes from a model: HTML in it must not reach the page.
// A fork's report is an item of its own: its sender, its Markdown rendered
// and escaped, its fork linked only for a viewer who is a member of it.
func TestBuildThread_ForkReport(t *testing.T) {
	report := func(id int64, fork string) store.MessageWithID {
		return store.MessageWithID{ID: id, CreatedAt: t0, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkReport,
			Content: j("## Fait\n<script>x</script>"), UserID: "u2", Author: "Victor",
			Fork: &store.ForkRef{SessionID: fork, Title: "Export CSV", UpToMessageID: 9}}}
	}
	items := BuildThread([]store.MessageWithID{report(4, "f1"), report(5, "gone")}, "u1",
		map[int64][]ForkLink{2: {{SessionID: "f1", Title: "Export CSV"}}}, nil, AgentDirectory{})
	if len(items) != 2 {
		t.Fatalf("%d items", len(items))
	}
	it := items[0]
	if it.Kind != ItemReport || it.ID != 4 || it.Author.Name != "Victor" || it.Mine ||
		it.Report != (ReportLink{SessionID: "f1", Title: "Export CSV", Accessible: true}) {
		t.Errorf("item %+v", it)
	}
	if !strings.Contains(string(it.HTML), "<h2>Fait</h2>") || strings.Contains(string(it.HTML), "<script>") {
		t.Errorf("html %s", it.HTML)
	}
	// A fork the viewer is not a member of, or deleted: named, not linked.
	if items[1].Report.Accessible || items[1].Report.Title != "Export CSV" {
		t.Errorf("unreachable fork %+v", items[1].Report)
	}
}

// The mark of the latest report goes after the last item it covers, before
// the questions waiting; the parent is linked only for its members.
func TestMarkReported(t *testing.T) {
	items := []ThreadItem{{Kind: ItemBrief, ID: 1}, {Kind: ItemHuman, ID: 2}, {Kind: ItemAgent, ID: 5}, {Kind: ItemHuman, ID: 8}, {Kind: ItemQuestion}}
	at := t0
	fork := store.Session{SessionID: "f", ParentSessionID: "p", LastReportedMessageID: 6, LastReportID: 40, LastReportedAt: &at}
	got := MarkReported(append([]ThreadItem(nil), items...), fork, true)
	if len(got) != 6 || got[3].Kind != ItemReported || got[4].ID != 8 ||
		got[3].Report != (ReportLink{SessionID: "p", MessageID: 40, Accessible: true}) || !got[3].Time.Equal(t0) {
		t.Errorf("marked %+v", got)
	}
	if got := MarkReported(items, fork, false); got[3].Report.Accessible {
		t.Error("the parent linked for a non-member")
	}
	fork.LastReportedMessageID = 99 // past the thread: before the question
	if got := MarkReported(items, fork, true); got[4].Kind != ItemReported || got[5].Kind != ItemQuestion {
		t.Errorf("marked %+v", got)
	}
	if got := MarkReported(items, store.Session{SessionID: "f"}, true); len(got) != len(items) {
		t.Error("a fork that never reported is marked")
	}
	// The items handed in are left as they were.
	if items[3].Kind != ItemHuman {
		t.Error("MarkReported changed its input")
	}
}

// In the chronological thread, the mark still goes after the last item the
// report covers (ID up to its last message), in the order shown: a report
// made while Jarvis's turn ran, after Smith had answered Bob, comes after
// Smith's answer; one made before Bob wrote, right after Alice's message;
// one made after Bob's message, after it. Jarvis's answer still running,
// covered in part, puts it at the end, before the questions.
func TestMarkReported_InChronologicalOrder(t *testing.T) {
	at := t0
	for _, c := range []struct {
		done  bool
		upTo  int64
		after int64 // the item the mark follows
	}{{true, 8, 7}, {true, 3, 1}, {true, 5, 4}, {true, 10, 9}, {false, 8, 2}, {false, 1, 1}} {
		items := BuildThread(parallel(c.done), "u-alice", nil, []Question{{WorkflowID: "q"}}, AgentDirectory{})
		fork := store.Session{SessionID: "f", ParentSessionID: "p", LastReportedMessageID: c.upTo, LastReportID: 40, LastReportedAt: &at}
		got := MarkReported(items, fork, true)
		i := slices.IndexFunc(got, func(it ThreadItem) bool { return it.Kind == ItemReported })
		if i < 1 || got[i-1].ID != c.after || got[len(got)-1].Kind != ItemQuestion {
			t.Errorf("done %v, reported up to %d: mark after %s, want after item #%d", c.done, c.upTo, shape(got[:max(i, 0)]), c.after)
		}
	}
}

func TestMarkdownIsSafe(t *testing.T) {
	out := string(Markdown("hi <script>alert(1)</script> [x](javascript:alert(1)) **bold**"))
	if strings.Contains(out, "<script>") || strings.Contains(out, "javascript:") {
		t.Errorf("unsafe output %s", out)
	}
	if !strings.Contains(out, "<strong>bold</strong>") {
		t.Errorf("markdown not rendered %s", out)
	}
}

func TestBuildMap(t *testing.T) {
	roots := BuildTree([]store.Session{
		sess("root", "", 0, t0), sess("a", "root", 1, t0), sess("b", "root", 2, t0), sess("a1", "a", 3, t0),
	}, nil, nil, "")
	m := BuildMap(roots[0])
	pos := map[string]MapNode{}
	for _, n := range m.Nodes {
		pos[n.Session.SessionID] = n
	}
	// Two leaves (a1, b) side by side; a over a1; root centred over a and b.
	if pos["a1"].X >= pos["b"].X || pos["a"].X != pos["a1"].X || pos["root"].X != (pos["a"].X+pos["b"].X)/2 {
		t.Errorf("x: %+v", pos)
	}
	if pos["root"].Y >= pos["a"].Y || pos["a"].Y >= pos["a1"].Y || pos["a"].Y != pos["b"].Y {
		t.Errorf("y: %+v", pos)
	}
	if len(m.Edges) != 3 || m.Nodes[0].Session.SessionID != "root" {
		t.Errorf("edges %d, first node %s", len(m.Edges), m.Nodes[0].Session.SessionID)
	}
	if m.Width < pos["b"].X+MapNodeW || m.Height < pos["a1"].Y+MapNodeH {
		t.Errorf("size %dx%d too small", m.Width, m.Height)
	}
}

func TestInitials(t *testing.T) {
	for name, want := range map[string]string{"Victor Freches": "VF", "bob@example.com": "BO", "Élodie": "ÉL", "": "?", "jean-paul sartre": "JP"} {
		if got := initials(name); got != want {
			t.Errorf("initials(%q) = %q, want %q", name, got, want)
		}
	}
}

// The Agents panel: the session's agent first, available when it has no
// participant, then the others; each says whom it answers and since when.
// A member may stop a turn answering them, the creator any, and only the
// creator drops a queue.
func TestBuildAgents(t *testing.T) {
	since := time.Date(2026, 10, 4, 16, 27, 0, 0, time.Local)
	directory := AgentDirectory{ByID: map[string]AgentInfo{"jarvis": {ID: "jarvis", Name: "Jarvis", Mention: "jarvis"}, "smith": smith}, Session: AgentInfo{ID: "jarvis", Name: "Jarvis", Mention: "jarvis"}}
	sess := store.Session{SessionID: "s1", CreatedBy: "u-alice"}
	ps := []session.Participant{
		{Participant: "smith", AgentID: "smith", Name: "Old Smith", Working: true, Turn: "m4.smith", UserID: "u-bob", UserName: "Bob", Since: since, Queued: 2, Note: "attend un worker"},
		{Participant: "watson", AgentID: "watson", Name: "Watson", Working: true, Waiting: true, Turn: "m5.watson", UserID: "u-carol", UserName: "Carol", Since: since},
		{Participant: "zed", AgentID: "zed", Queued: 1},
	}

	bob := BuildAgents(ps, sess, "u-bob", directory)
	var got []string
	for _, r := range bob.Rows {
		got = append(got, fmt.Sprintf("%s|%s|%s|%v|%v", r.Participant, r.Agent.Name, r.Status(), r.CanStop, r.CanClear))
	}
	want := []string{
		"jarvis|Jarvis|Disponible|false|false",
		"smith|Agent Smith|Te répond depuis " + clock(since) + "|true|false",
		"watson|Watson|Répond à Carol depuis " + clock(since) + " · attend une réponse|false|false",
		"zed|zed|Passe au message suivant|false|false",
	}
	if !slices.Equal(got, want) {
		t.Errorf("bob's panel\n got %q\nwant %q", got, want)
	}
	if !bob.CanStop || bob.Rows[1].Turn != "m4.smith" || bob.Rows[2].State() != StatusWaiting || bob.Rows[1].State() != StatusWorking || bob.Rows[0].State() != StatusIdle {
		t.Errorf("bob's panel %+v", bob)
	}
	if w := bob.Working(); len(w) != 2 || w[0] != (WorkingAgent{Name: "Agent Smith", Note: "attend un worker"}) || w[1].Name != "Watson" {
		t.Errorf("working %+v", w)
	}

	alice := BuildAgents(ps, sess, "u-alice", directory)
	for _, r := range alice.Rows[1:] {
		if r.CanStop != r.Working || !r.CanClear {
			t.Errorf("the creator's row %+v", r)
		}
	}
	if alice.Rows[0].CanClear || alice.Rows[1].ClearConfirm() != "Arrêter Agent Smith et jeter 2 messages de sa file, ceux des autres membres compris ?" {
		t.Errorf("the creator's rows %+v", alice.Rows)
	}

	dave := BuildAgents(ps[1:2], sess, "u-dave", directory)
	if dave.CanStop || len(dave.Rows) != 2 || dave.Rows[1].CanStop {
		t.Errorf("dave's panel %+v", dave)
	}

	// At work on a turn nothing described: no author, no start.
	if got := (AgentRow{Working: true}).Status(); got != "En cours" {
		t.Errorf("an undescribed turn: %q", got)
	}

	// The session's agent at work keeps its row, first.
	ps[0].Participant, ps[0].AgentID = "jarvis", "jarvis"
	if rows := BuildAgents(ps, sess, "u-bob", directory).Rows; len(rows) != 3 || rows[0].Participant != "jarvis" || !rows[0].Working {
		t.Errorf("rows %+v", rows)
	}
}

// A turn's files go under its answer, else under its error; a turn the
// thread does not show yet gets them on an item of its own, at the end,
// before the questions.
func TestAttachFiles(t *testing.T) {
	jarvis := AgentInfo{ID: "jarvis", Name: "Jarvis", Mention: "jarvis"}
	smith := AgentInfo{ID: "smith", Name: "Agent Smith"}
	answered := store.TurnKey(1, "jarvis")
	failed := store.TurnKey(1, "smith")
	silent := store.TurnKey(1, "writer")
	msgs := []store.MessageWithID{
		{ID: 1, Key: store.HumanMessageKey("a"), Message: store.Message{Role: store.RoleUser, Content: j("@jarvis @smith a report"), UserID: "u-me"}},
		{ID: 2, Key: store.TurnMessageKey(answered, 0), Message: store.Message{Role: store.RoleAssistant, AgentID: "jarvis", ToolCalls: []store.ToolCall{{ID: "c1", Name: "publish_file"}}}},
		{ID: 3, Key: store.TurnMessageKey(answered, 1), Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "c1", Content: "Published"}}},
		{ID: 4, Key: store.TurnMessageKey(answered, 2), Message: store.Message{Role: store.RoleAssistant, AgentID: "jarvis", Content: j("Here it is.")}},
		{ID: 5, Key: store.TurnEndKey(answered), Message: store.TurnEnd("jarvis", "")},
		{ID: 6, Key: store.TurnEndKey(failed), Message: store.TurnEnd("smith", "call LLM: boom")},
	}
	directory := AgentDirectory{ByID: map[string]AgentInfo{"jarvis": jarvis, "smith": smith}, Session: jarvis}
	items := BuildThread(msgs, "u-me", nil, []Question{{WorkflowID: "q1", Text: "Which?"}}, directory)
	files := []store.File{
		{ID: "f1", TurnKey: answered, AgentID: "jarvis", Name: "rapport.md", Size: 1536},
		{ID: "f2", TurnKey: failed, AgentID: "smith", Name: "partial.csv", Size: 12},
		{ID: "f3", TurnKey: silent, AgentID: "writer", Name: "draft.md", Size: 3 << 20, CreatedAt: t0},
		{ID: "f4", TurnKey: answered, AgentID: "jarvis", Name: "data.json", Size: 2, SHA256: "d1"},
		// Published again by the model, same name and content: shown once.
		{ID: "f5", TurnKey: answered, AgentID: "jarvis", Name: "data.json", Size: 2, SHA256: "d1"},
		// Same name, new content: shown.
		{ID: "f6", TurnKey: answered, AgentID: "jarvis", Name: "data.json", Size: 3, SHA256: "d2"},
		// By a sub-agent of the turn: says so.
		{ID: "f7", TurnKey: answered, AgentID: "smith", Name: "chart.svg", Size: 5},
	}
	items = AttachFiles(items, files, directory)

	var kinds []string
	for _, it := range items {
		kinds = append(kinds, it.Kind)
	}
	if got := strings.Join(kinds, ","); got != "human,agent,error,files,question" {
		t.Fatalf("kinds %s", got)
	}
	want := []FileLink{{ID: "f1", Name: "rapport.md", Size: "1,5 Ko"}, {ID: "f4", Name: "data.json", Size: "2 o"},
		{ID: "f6", Name: "data.json", Size: "3 o"}, {ID: "f7", Name: "chart.svg", Size: "5 o", Via: "Agent Smith"}}
	if !reflect.DeepEqual(items[1].Files, want) {
		t.Errorf("answer's files %+v", items[1].Files)
	}
	if len(items[2].Files) != 1 || items[2].Files[0].ID != "f2" {
		t.Errorf("error's files %+v", items[2].Files)
	}
	orphan := items[3]
	if len(orphan.Files) != 1 || orphan.Files[0].Size != "3,0 Mo" || orphan.Agent.Name != "writer" || !orphan.Time.Equal(t0) || orphan.ID != 0 {
		t.Errorf("orphan %+v", orphan)
	}
	if items[1].Files[0].Href() != "/files/f1" {
		t.Errorf("href %s", items[1].Files[0].Href())
	}
	// No files: nothing changes.
	if got := AttachFiles(items[:1], nil, directory); len(got) != 1 {
		t.Errorf("no files: %+v", got)
	}
}

// An answer whose model ran on a machine says whose, and which model; one
// that fell back to the server's key says that too; one on the server's key
// alone says nothing.
func TestBuildThread_SaysTheMachine(t *testing.T) {
	jarvis := AgentInfo{ID: "default", Name: "Jarvis"}
	msgs := []store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Content: j("Q1"), UserID: "u-v", Author: "Victor"}},
		{ID: 2, Message: store.Message{Role: store.RoleAssistant, Content: j("R1"), AgentID: "default", UserID: "u-v", MachineID: "m1", Machine: "portable", Model: "sonnet"}},
		{ID: 3, Message: store.TurnEnd("default", "")},
		{ID: 4, Message: store.Message{Role: store.RoleUser, Content: j("Q2"), UserID: "u-v", Author: "Victor"}},
		{ID: 5, Message: store.Message{Role: store.RoleAssistant, AgentID: "default", UserID: "u-x", MachineID: "m2", Machine: "maison", ToolCalls: []store.ToolCall{{ID: "t", Name: "exec"}}}},
		{ID: 6, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t"}}},
		{ID: 7, Message: store.Message{Role: store.RoleAssistant, Content: j("R2"), AgentID: "default", UserID: "u-x", Model: "claude-sonnet-5"}},
		{ID: 8, Message: store.TurnEnd("default", "")},
		{ID: 9, Message: store.Message{Role: store.RoleUser, Content: j("Q3"), UserID: "u-v", Author: "Victor"}},
		{ID: 10, Message: store.Message{Role: store.RoleAssistant, Content: j("R3"), AgentID: "default", Model: "claude-sonnet-5"}},
	}
	items := BuildThread(msgs, "u-v", nil, nil, AgentDirectory{Session: jarvis})
	var got []string
	for _, it := range items {
		if it.Kind == ItemAgent {
			got = append(got, it.Via())
		}
	}
	want := []string{"via la machine de Victor · sonnet", "via la machine « maison » puis le modèle de l'installation", ""}
	if !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
