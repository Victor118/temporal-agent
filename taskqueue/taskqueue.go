// Package taskqueue reports who serves a Temporal task queue, from the pollers
// Temporal has seen. Temporal is the source of truth for liveness: there is no
// heartbeat of our own.
package taskqueue

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Freshness is how recent a poller must be to count. Temporal itself drops
// pollers unseen for about 5 minutes.
const Freshness = 5 * time.Minute

// Poller is a worker Temporal has seen polling the queue.
type Poller struct {
	Identity   string
	LastAccess time.Time
}

// Status lists the pollers of one queue, by task type. Err is set when
// Temporal could not be asked: the pollers are then unknown, not absent.
type Status struct {
	Queue    string
	Activity []Poller
	Workflow []Poller
	Err      error
}

// Describe asks Temporal for the activity and workflow pollers of queue.
func Describe(ctx context.Context, tc client.Client, queue string) Status {
	s := Status{Queue: queue}
	for _, typ := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY, enumspb.TASK_QUEUE_TYPE_WORKFLOW} {
		resp, err := tc.DescribeTaskQueue(ctx, queue, typ)
		if err != nil {
			return Status{Queue: queue, Err: err}
		}
		var pollers []Poller
		for _, p := range resp.GetPollers() {
			pollers = append(pollers, Poller{Identity: p.GetIdentity(), LastAccess: p.GetLastAccessTime().AsTime()})
		}
		if typ == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			s.Activity = pollers
		} else {
			s.Workflow = pollers
		}
	}
	return s
}

// ActivityServed reports whether a fresh poller takes activity tasks.
func (s Status) ActivityServed() bool { return anyFresh(s.Activity) }

// WorkflowServed reports whether a fresh poller takes workflow tasks.
func (s Status) WorkflowServed() bool { return anyFresh(s.Workflow) }

// Served reports whether any fresh poller serves the queue.
func (s Status) Served() bool { return s.ActivityServed() || s.WorkflowServed() }

func anyFresh(pollers []Poller) bool {
	for _, p := range pollers {
		if time.Since(p.LastAccess) < Freshness {
			return true
		}
	}
	return false
}
