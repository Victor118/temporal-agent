package subproc

// GitSafeArgs come before every git command started around a coding run, on
// a worker or a user's machine: whatever a tree holds must not make git run
// a program — no hooks, no filesystem monitor, no submodule fetched behind
// the command's back.
var GitSafeArgs = []string{"-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "submodule.recurse=false"}

// GitEnv is the environment of such a git command: environ filtered (Env),
// plus the names given, and neither the system's nor the user's git
// configuration (what git does is the code's decision, not a config file's).
func GitEnv(environ []string, names ...string) []string {
	return append(Env(environ, names, nil), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
}
