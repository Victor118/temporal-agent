#!/usr/bin/env bash
# Does a Bash command write source files? One judgement for the test-first guard and eval-mechanics.sh.
#   sourced:   . bash-writes.sh; bash_writes "$command" "$SOURCE_PATH"   (or "$TEST_PATH")
#   executed:  bash-writes.sh source|test < command   → exit 0 when it writes, 1 otherwise
#
# bash_writes <command> <path regex> → 0 when one segment of the command both names a matching path and
# writes. Segments are split on && || ; | (a heredoc or a shell variable keeps the whole command as one
# segment), so `mv .claude/state/a b && git diff access/x.go` writes nothing. A redirect or tee whose
# target neither matches the regex nor is a variable ($F) is dropped, so `git diff access/x.go > /tmp/d`
# writes nothing, and so is the body of `cat > <that target> <<DELIM` (a note in /tmp may name a test file).
#
# Source = Go files: production *.go and tests *_test.go (and testdata/). Test = *_test.go and testdata/.
# `go fmt`, `go fix`, `go generate` and `gofmt -w` / `goimports -w` rewrite Go files without naming one:
# they count as writing source (not tests: the refactor guard judges what they actually changed afterwards).

GO_REWRITE='\bgo +(fmt|fix|generate)\b|\b(gofmt|goimports)\b[^|;&]*[[:space:]]-[a-z]*w'
WRITE_OPERATOR='>|\btee\b|python3? +-|\bsed +-i|\bperl +-[a-z]*i|\bcp |\bmv |\brm |\btouch |open\(|\bgit +(checkout|restore|apply|mv|rm|stash)\b|'"$GO_REWRITE"
TEST_PATH='_test\.go\b|(^|[[:space:]/"'"'"'=])testdata/'
SOURCE_PATH='\.go\b|(^|[[:space:]/"'"'"'=])testdata/|'"$GO_REWRITE"

without_foreign_heredocs() {  # stdin = command, $1 = path regex
  local line delimiter="" pattern='cat[[:space:]]*>[[:space:]]*([^[:space:]$<>]+)[[:space:]]*<<-?[[:space:]]*["'"'"']?([A-Za-z_]+)'
  while IFS= read -r line; do
    if [ -n "$delimiter" ]; then [ "$line" = "$delimiter" ] && delimiter=""; continue; fi
    printf '%s\n' "$line"
    if [[ $line =~ $pattern ]] && ! grep -qE "$1" <<<"${BASH_REMATCH[1]}"; then delimiter=${BASH_REMATCH[2]}; fi
  done
}

bash_writes() {
  local command segment target
  command=$(sed -E 's#[0-9]?>&[0-9]|[0-9]?>[[:space:]]*/dev/null##g' <<<"$1" | without_foreign_heredocs "$2")
  if grep -qE '<<|[$][A-Za-z_{]' <<<"$command"; then   # heredoc or variables: segments may depend on each other
    command=$(tr '\n' ' ' <<<"$command")
  else
    command=$(sed -E 's/&&|\|\||;|\|/\n/g' <<<"$command")
  fi
  while IFS= read -r segment; do
    grep -qE '^[[:space:]]*(\S*/)?\.claude/scripts/tdd-run\.sh' <<<"$segment" && continue
    while IFS= read -r target; do
      [ -z "$target" ] && continue
      grep -qE "$2|[\$]" <<<"${target##*[[:space:]]}" || segment=${segment//"$target"/}
    done < <(grep -oE '([0-9]?>>?|\btee( +-a)?)[[:space:]]*[^[:space:]<>]+' <<<"$segment" | grep -v '<<')
    grep -qE "$2" <<<"$segment" && grep -qE "$WRITE_OPERATOR" <<<"$segment" && return 0
  done <<<"$command"
  return 1
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  case "${1:?source|test}" in
    source) bash_writes "$(cat)" "$SOURCE_PATH" ;;
    test) bash_writes "$(cat)" "$TEST_PATH" ;;
    *) echo "usage: bash-writes.sh source|test < command" >&2; exit 2 ;;
  esac
fi
