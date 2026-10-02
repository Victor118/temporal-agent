package main

import (
	"fmt"

	"github.com/google/uuid"
)

// Temporal's visibility queries are text, and a session ID goes into them
// between quotes. The routes check membership first, which a forged ID never
// passes, but that protection is indirect: these builders accept a session ID
// only in its canonical UUID form, which holds no quote, whatever the caller
// checked before.

// checkSessionID refuses anything but a canonical UUID.
func checkSessionID(sessionID string) error {
	if id, err := uuid.Parse(sessionID); err != nil || id.String() != sessionID {
		return fmt.Errorf("invalid session ID %q", sessionID)
	}
	return nil
}

// runningSessionQuery finds the session's own workflow, first run or resumed
// ("session-<id>", "session-<id>-<unix time>").
func runningSessionQuery(sessionID string) (string, error) {
	if err := checkSessionID(sessionID); err != nil {
		return "", err
	}
	return fmt.Sprintf("WorkflowId STARTS_WITH 'session-%s' AND ExecutionStatus = 'Running'", sessionID), nil
}

// pendingQuestionsQuery finds the questions waiting in a session, its agent's
// and its sub-agents' alike: their IDs all start with the session's.
func pendingQuestionsQuery(sessionID string) (string, error) {
	if err := checkSessionID(sessionID); err != nil {
		return "", err
	}
	return fmt.Sprintf("WorkflowType = 'AskUserWorkflow' AND ExecutionStatus = 'Running' AND WorkflowId STARTS_WITH '%s-'", sessionID), nil
}
