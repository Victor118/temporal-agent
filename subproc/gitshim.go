package subproc

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// gitShim is the git a coding run's CLI finds first in its PATH (WriteGitShim).
// The run may use git add, commit and status; their options that read a file
// — commit's -F/--file and -t/--template, add's and commit's
// --pathspec-from-file, under any unambiguous abbreviation, attached or not,
// in a bundle of short options — would put any file its user can read in a
// commit, and the commit in a pushed branch. Their file must be in the clone
// (resolved, links followed); the standard input ("-") is refused, a message
// goes by -m. Everything else goes to the real git as it is. Its git and
// readlink are absolute paths, set when it is written: the run's PATH picks
// neither.
const gitShim = `#!/bin/sh
# git for a coding run: a commit's message or pathspec is read from the clone
# only (written by the worker or agent connect, not the run's to change).
real=%s
readlink=%s
root=%s
refuse() {
	echo "git: $1: refused for this run (a file option must name a file of the clone; give the message with -m)" >&2
	exit 128
}
inside() {
	[ "$1" = "-" ] && refuse "reading the standard input"
	p=$("$readlink" -f -- "$1" 2>/dev/null) || refuse "$1"
	case "$p" in
	"$root" | "$root"/*) ;;
	*) refuse "$1 is outside the clone" ;;
	esac
}
fileopt() {
	[ ${#1} -ge 3 ] || return 1
	for o in file template pathspec-from-file; do
		case "$o" in "$1"*) return 0 ;; esac
	done
	return 1
}
case "$1" in
commit | add)
	first=1 want= skip=
	for a in "$@"; do
		if [ -n "$first" ]; then first=; continue; fi
		if [ -n "$want" ]; then inside "$a"; want=; continue; fi
		if [ -n "$skip" ]; then skip=; continue; fi
		case "$a" in
		--) break ;;
		--*=*)
			n=${a%%%%=*}
			fileopt "${n#--}" && inside "${a#*=}"
			;;
		--?*) fileopt "${a#--}" && want=1 ;;
		-?*)
			b=${a#-}
			while [ -n "$b" ]; do
				c=${b%%%%"${b#?}"}
				b=${b#?}
				case "$c" in
				F | t)
					if [ -n "$b" ]; then inside "$b"; else want=1; fi
					break
					;;
				m | c | C)
					[ -z "$b" ] && skip=1
					break
					;;
				esac
			done
			;;
		esac
	done
	[ -n "$want" ] && refuse "a file option without its file"
	;;
esac
exec "$real" "$@"
`

// GitShimDir is where WriteGitShim writes the git of the run whose clone is
// dir: next to it, never in it.
func GitShimDir(dir string) string { return dir + ".bin" }

// WriteGitShim writes, in GitShimDir(clone), a git that guards the file
// options of commit and add (gitShim) for a run working in clone, and
// returns the directory to put first in the run's PATH. The directory and
// the script belong to the caller and are not writable by others: a run
// under another user (RunAs) cannot change them, and a run under the same
// user (a machine) can write no file outside its working directories. git
// and readlink are found in this process's PATH, not the run's.
func WriteGitShim(clone string) (string, error) {
	realGit, err := exec.LookPath("git")
	if err != nil {
		return "", err
	}
	if realGit, err = filepath.Abs(realGit); err != nil {
		return "", err
	}
	readlink, err := exec.LookPath("readlink")
	if err != nil {
		return "", err
	}
	if readlink, err = filepath.Abs(readlink); err != nil {
		return "", err
	}
	root, err := filepath.EvalSymlinks(clone)
	if err != nil {
		return "", err
	}
	dir := GitShimDir(clone)
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		return "", err
	}
	script := fmt.Sprintf(gitShim, shellQuote(realGit), shellQuote(readlink), shellQuote(root))
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(script), 0o555); err != nil {
		return "", err
	}
	return dir, nil
}

// WithPath is environ's PATH with dir first (environ's PATH, else this
// process's), as an entry to append: the last one wins.
func WithPath(environ []string, dir string) string {
	path := os.Getenv("PATH")
	for _, kv := range environ {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			path = v
		}
	}
	return "PATH=" + dir + string(os.PathListSeparator) + path
}

// shellQuote quotes s for sh, single quotes and all.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
