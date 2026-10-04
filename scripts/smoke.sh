#!/usr/bin/env bash
# Smoke run of the real binary: `agent dev` against the compose Temporal and
# PostgreSQL, on a task queue and a database of its own, with an LLM key that
# is invalid on purpose (each turn fails at its first call, which still runs
# the whole path: delivery, participant, turn, its end, the thread's error).
#
# It never touches the `agent` database, the real queues or the running
# services: the database agent_smoke_<rand> is created then dropped, the
# binary is built and run under /tmp/smoke-<rand> in the agent container (on
# 127.0.0.1:18888), and both are removed on exit, failure included.
#
# Run from anywhere, compose up:  agent/scripts/smoke.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

R=$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
DB=agent_smoke_$R
QUEUE=smoke-$R
DIR=/tmp/smoke-$R

cleanup() {
	docker compose exec -T agent sh -c "pkill -f '^$DIR/agent dev' 2>/dev/null; rm -rf '$DIR'" || true
	docker compose exec -T postgres dropdb -U agent --if-exists "$DB" || true
	echo "cleaned: database $DB dropped, $DIR removed"
}
trap cleanup EXIT

docker compose exec -T postgres createdb -U agent "$DB"
echo "database $DB, queue $QUEUE, dir $DIR"

docker compose exec -T -e DB="$DB" -e QUEUE="$QUEUE" -e DIR="$DIR" agent sh -s <<'INNER'
set -eu
fail() { echo "FAIL: $*"; exit 1; }
mkdir -p "$DIR/workspace" "$DIR/claude-code" "$DIR/claude-config"
go build -o "$DIR/agent" ./cmd/agent

export DATABASE_URL="postgres://agent:agent@postgres:5432/$DB?sslmode=disable"
export WORKFLOW_QUEUE="$QUEUE" HTTP_ADDR=127.0.0.1:18888 INTERNAL_ADDR=127.0.0.1:19999
export WORKER_CONFIG=/nonexistent SKILLS_REPO= MCP_SERVERS= BRAVE_SEARCH_API_KEY=
export WORKSPACE_PATH="$DIR/workspace" CLAUDE_CODE_WORKSPACE="$DIR/claude-code" CLAUDE_CONFIG_DIR="$DIR/claude-config"
export TELEGRAM_BOT_TOKEN= TELEGRAM_WEBHOOK_SECRET= SKILLS_WEBHOOK_SECRET= SMTP_HOST=
export LLM_API_KEY=smoke-invalid-key
# No run-as user: this process shares the container of the real dev worker,
# whose subproc.Runs would end any process of the same uid.
export RUN_AS_UID= RUN_AS_GID=

"$DIR/agent" dev >"$DIR/log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null || true' EXIT

B=http://127.0.0.1:18888
for i in $(seq 1 60); do
	curl -s -o /dev/null "$B/auth/me" && break
	kill -0 $PID 2>/dev/null || { cat "$DIR/log"; fail "agent dev exited"; }
	sleep 1
done
grep -q "Worker registered on task queue \"$QUEUE\"" "$DIR/log" || { cat "$DIR/log"; fail "no worker on $QUEUE"; }
queues=$(grep -o 'TaskQueue [^ ]*' "$DIR/log" | sort -u | tr '\n' ' ')
[ "$queues" = "TaskQueue $QUEUE " ] || fail "workers on other queues: $queues"

echo smoke-password-0123456789 | "$DIR/agent" user create --email smoke@example.com --admin --password-stdin >/dev/null
J="$DIR/cookies"
api() { # method path [body]: prints the body, then the status on its own line
	curl -s -b "$J" -c "$J" -H "Origin: $B" -H 'Content-Type: application/json' -X "$1" ${3:+-d "$3"} -w '\n%{http_code}' "$B$2"
}
status() { tail -n1; }
body() { sed '$d'; }

[ "$(api POST /auth/login '{"email":"smoke@example.com","password":"smoke-password-0123456789"}' | status)" = 200 ] || fail login
SID=$(api POST /sessions '{}' | body | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
[ -n "$SID" ] || fail "no session"
echo "session $SID"

for text in bonjour encore; do
	out=$(api POST "/sessions/$SID/messages" "{\"content\":\"$text\"}")
	echo "send $text: $(echo "$out" | body) $(echo "$out" | status)"
	[ "$(echo "$out" | status)" = 202 ] && echo "$out" | grep -q '"agent_called":true' || fail "send $text"
done
echo "state during: $(api GET "/sessions/$SID/state" | body)"

# Both turns fail on the invalid key: two turn errors in the history.
errors=0
for i in $(seq 1 90); do
	errors=$(api GET "/sessions/$SID/history" | body | grep -o '"type":"turn_error"' | wc -l)
	[ "$errors" -ge 2 ] && break
	sleep 1
done
history=$(api GET "/sessions/$SID/history" | body)
echo "history: $history"
[ "$errors" -eq 2 ] || fail "$errors turn errors, want 2"
echo "$history" | grep -q '"content":"bonjour"' && echo "$history" | grep -q '"content":"encore"' || fail "messages missing"
grep -q "ParticipantWorkflow.*$SID:p:" "$DIR/log" || fail "no participant in the log"

# The participant ends once its inbox is empty.
for i in $(seq 1 30); do
	state=$(api GET "/sessions/$SID/state" | body)
	[ "$state" = "[]" ] && break
	sleep 1
done
echo "state after: $state"
[ "$state" = "[]" ] || fail "participant still running"

out=$(api POST "/sessions/$SID/cancel")
echo "stop: $(echo "$out" | body) $(echo "$out" | status)"
[ "$(echo "$out" | status)" = 404 ] || fail "stop with no turn"
api GET /me/sessions | body | grep -q '"active"' && fail '"active" still listed'
[ "$(api DELETE "/sessions/$SID" | status)" = 204 ] || fail delete

kill $PID; wait $PID 2>/dev/null || true
trap - EXIT
panics=$(grep -c '^panic' "$DIR/log" || true)
[ "$panics" -eq 0 ] || { cat "$DIR/log"; fail "$panics panics"; }
echo "PASS"
INNER
