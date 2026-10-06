package subproc

// GitSafeArgs come before every git command started around a coding run, on
// a worker or a user's machine: whatever a tree holds must not make git run
// a program — no hooks, no filesystem monitor, no submodule fetched behind
// the command's back.
var GitSafeArgs = []string{"-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "submodule.recurse=false"}

// GitProtocolArgs keep a clone to the transports a repository's URL may
// name on a user's machine: ssh, https and local paths. Never ext:: (it runs
// a command), nor plaintext git:// or http://. On the command line, they win
// over the user's configuration.
var GitProtocolArgs = []string{
	"-c", "protocol.allow=never",
	"-c", "protocol.ext.allow=never",
	"-c", "protocol.ssh.allow=always",
	"-c", "protocol.https.allow=always",
	"-c", "protocol.file.allow=always",
}

// noPrompt: git never waits on a terminal for a user name or a password.
const noPrompt = "GIT_TERMINAL_PROMPT=0"

// GitEnv is the environment of a git command on a worker: environ filtered
// (Env), plus the names given, and neither the system's nor the user's git
// configuration (what git does is the code's decision, not a config file's),
// nor any prompt.
func GitEnv(environ []string, names ...string) []string {
	return append(Env(environ, names, nil), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", noPrompt)
}

// GitEnvUser is the environment of a git command on a user's machine, run
// for them: environ filtered as GitEnv does, but their system and global git
// configuration read (their credential helpers, their ssh setup), and no
// prompt either. What a repository's own configuration could make git do is
// still refused by GitSafeArgs and GitProtocolArgs.
func GitEnvUser(environ []string, names ...string) []string {
	return append(Env(environ, names, nil), noPrompt)
}
