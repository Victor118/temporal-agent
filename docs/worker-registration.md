# Feature: Worker Registration & Dynamic Task Queue Assignment

## Overview

Workers register themselves on the API server with a persistent ID and capability tags. The server assigns task queues to workers based on their tags. Workers poll the API to discover their queues and dynamically start/stop Temporal workers.

## How it works

1. At first launch, the worker generates a UUID and persists it locally (`./data/worker-id`)
2. The worker calls `POST /workers/register` with its ID and tags (e.g. `["gpu", "eu-west"]`)
3. The server stores/updates the worker record in the DB
4. The worker polls `GET /workers/me/task-queues` periodically (e.g. every 30s)
5. The server matches the worker's tags against task queue assignments and returns the list
6. The worker creates new Temporal workers for new queues, stops workers for removed queues
7. The worker sends periodic heartbeats (same register endpoint) so the server knows it's alive

## Worker Identity

```
First launch:
  → generate UUID "worker-a3f8b2..."
  → save to ./data/worker-id
  → POST /workers/register

Restart:
  → read ./data/worker-id → "worker-a3f8b2..."
  → POST /workers/register (same ID, server updates heartbeat)
```

The ID is passed via `X-Worker-ID` header on all API calls.

## API Endpoints

### `POST /workers/register`

Register or refresh a worker.

```json
// Request
{
  "id": "worker-a3f8b2...",
  "tags": ["gpu", "eu-west"],
  "version": "1.0.0"
}

// Response
{
  "id": "worker-a3f8b2...",
  "status": "active"
}
```

### `GET /workers/me/task-queues`

Returns the task queues assigned to this worker (based on its tags).

```json
// Response
{
  "queues": ["agent-default", "agent-gpu"]
}
```

### `GET /workers` (dashboard)

List all known workers with their status.

```json
{
  "workers": [
    {
      "id": "worker-a3f8b2...",
      "tags": ["gpu", "eu-west"],
      "status": "active",
      "last_heartbeat": "2026-03-19T10:00:00Z",
      "task_queues": ["agent-default", "agent-gpu"]
    }
  ]
}
```

### `POST /task-queues` (dashboard)

Create or update a task queue assignment.

```json
// Request
{
  "queue_name": "agent-gpu",
  "required_tags": ["gpu"]
}
```

### `DELETE /task-queues/{name}` (dashboard)

Remove a task queue assignment. Workers listening on this queue will stop their worker for it on next poll.

### `GET /task-queues` (dashboard)

List all configured task queues and their tag requirements.

```json
{
  "queues": [
    { "name": "agent-default", "required_tags": [] },
    { "name": "agent-gpu", "required_tags": ["gpu"] }
  ]
}
```

## Database Schema

```sql
CREATE TABLE workers (
    id TEXT PRIMARY KEY,
    tags TEXT NOT NULL DEFAULT '[]',  -- JSON array
    version TEXT,
    status TEXT NOT NULL DEFAULT 'active',  -- 'active', 'dead'
    last_heartbeat DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE task_queue_config (
    queue_name TEXT PRIMARY KEY,
    required_tags TEXT NOT NULL DEFAULT '[]',  -- JSON array, empty = all workers
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

## Queue Assignment Logic

A worker receives a queue if:
- The queue has **no required tags** (assigned to all workers), OR
- The worker's tags **contain all** the queue's required tags

```go
func matchQueue(workerTags []string, requiredTags []string) bool {
    if len(requiredTags) == 0 {
        return true
    }
    tagSet := make(map[string]bool, len(workerTags))
    for _, t := range workerTags {
        tagSet[t] = true
    }
    for _, req := range requiredTags {
        if !tagSet[req] {
            return false
        }
    }
    return true
}
```

## Worker Dynamic Loop

```go
func (w *WorkerManager) pollLoop(ctx context.Context) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            queues := w.fetchAssignedQueues()
            w.reconcile(queues) // start new, stop removed
        }
    }
}
```

## Worker Tags Configuration

Tags are set via environment variable at worker startup:

```bash
WORKER_TAGS=gpu,eu-west
```

## Dead Worker Detection

The server marks workers as `dead` if their last heartbeat is older than 2 minutes. The dashboard shows this status. Dead workers are not deleted — they reactivate on next register call.

## Example Flow

```
# Admin creates queues via dashboard
POST /task-queues {"queue_name": "agent-default", "required_tags": []}
POST /task-queues {"queue_name": "agent-gpu", "required_tags": ["gpu"]}
POST /task-queues {"queue_name": "agent-eu", "required_tags": ["eu-west"]}

# Worker A starts with tags: ["gpu", "eu-west"]
POST /workers/register {id: "worker-A", tags: ["gpu", "eu-west"]}
GET  /workers/me/task-queues → ["agent-default", "agent-gpu", "agent-eu"]
→ Starts 3 Temporal workers

# Worker B starts with tags: ["cpu"]
POST /workers/register {id: "worker-B", tags: ["cpu"]}
GET  /workers/me/task-queues → ["agent-default"]
→ Starts 1 Temporal worker

# Admin removes "agent-eu" queue via dashboard
DELETE /task-queues/agent-eu

# Worker A polls on next tick
GET /workers/me/task-queues → ["agent-default", "agent-gpu"]
→ Stops the "agent-eu" Temporal worker
```

## Notes

- `agent-default` (no required tags) is always assigned to every worker — it's the fallback queue
- The initial `TASK_QUEUES` env var still works as a static override (bypasses the API)
- In dev mode, the dynamic poll is disabled — queues come from config only
