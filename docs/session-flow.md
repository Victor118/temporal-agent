# Session flow — séquence d'un message utilisateur

Diagramme de séquence d'un tour complet : un user envoie un message, la session
charge son contexte, lance l'agent générique qui délègue à deux sous-agents
spécialisés, l'un d'eux pose une question à l'user, puis la réponse finale est
streamée vers l'UI.

```mermaid
sequenceDiagram
    autonumber
    actor U as User (web/telegram)
    participant H as HTTP handler<br/>(cmd/agent)
    participant T as Temporal
    participant SW as SessionWorkflow<br/>(long-lived)
    participant Mem as MemoryActivity<br/>(Postgres)
    participant GA as AgentWorkflow<br/>(generic queue)
    participant LLM as CallLLM activity
    participant SA1 as AgentWorkflow<br/>(market-analyst)
    participant SA2 as AgentWorkflow<br/>(code-reviewer)
    participant AU as AskUserWorkflow
    participant SSE as SSE Hub

    U->>H: POST /sessions/:id/messages
    H->>T: SignalWithStart(SessionWorkflow, "user-message")
    T-->>SW: start (or signal existing)

    Note over SW: processTurn — turn N
    SW->>Mem: LoadContext (messages + user memory)
    Mem-->>SW: history + memory
    SW->>GA: ExecuteChildWorkflow(AgentWorkflow)

    Note over GA: ReAct loop
    GA->>LLM: CallLLM(system + history + tools)
    LLM-->>GA: tool_calls = [spawn_session×2, ...]

    par Spécialiste 1
        GA->>SA1: ChildWorkflow on "market-analyst" queue
        SA1->>LLM: CallLLM (skills market-research)
        LLM-->>SA1: réponse finale
        SA1-->>GA: result string
    and Spécialiste 2
        GA->>SA2: ChildWorkflow on "code-reviewer" queue
        SA2->>LLM: CallLLM (skills code-review)
        LLM-->>SA2: tool_calls = [ask_user]
        SA2->>AU: ChildWorkflow(AskUserWorkflow)
        AU->>SSE: NotifyStep(type=ask_user, workflow_id)
        SSE-->>U: event "ask_user" (UI affiche question)
        U->>H: POST /ask/:wfID/answer
        H->>T: SignalWorkflow(AskUserWorkflow, "user-answer")
        T-->>AU: answer
        AU-->>SA2: answer string (tool result)
        SA2->>LLM: CallLLM (avec la réponse)
        LLM-->>SA2: réponse finale
        SA2-->>GA: result string
    end

    GA->>LLM: CallLLM (synthèse des 2 résultats)
    LLM-->>GA: réponse finale (no tool_calls)
    GA->>SSE: NotifyStep(type=message, content)
    SSE-->>U: event "message" (réponse streamée)
    GA-->>SW: AgentWorkflowOutput{messages, response}

    SW->>Mem: PersistContext(messages)
    Note over SW: status = idle, attend prochain signal
```

## Points clés

- **Generic agent et specific agents = même `AgentWorkflow`**, lancée sur des
  task queues différentes (`agent-default`, `market-analyst`, `code-reviewer`).
  La queue détermine les skills chargés via
  `workflow.GetInfo(ctx).TaskQueueName` → `LoadSkillsForQueue`.
- **`spawn_session`** est un *tool* implémenté comme **child workflow**
  (cf. `buildChildInput` dans `workflow/agent.go`), pas une activity. Le LLM
  choisit la queue cible via le champ `task_queue` de son tool input.
- **`ask_user`** est aussi un child workflow (`AskUserWorkflow`) qui bloque sur
  un signal Temporal. Son `WorkflowID` suit la convention
  `{sessionID}-tool-ask_user-{N}` pour que le SSE puisse router la question
  vers la bonne session UI même quand c'est un sous-agent qui la pose.
- Côté UI, le user voit **3 events SSE** dans l'ordre :
  `tool_calls` (le générique a appelé `spawn_session` ×2), `ask_user`
  (du sous-agent), puis `message` (la synthèse finale).
- La persistance est faite **uniquement par `SessionWorkflow`** après chaque
  turn, pas par les `AgentWorkflow` enfants. Les sous-agents n'ont pas de
  mémoire propre — leur historique meurt avec leur exécution.
