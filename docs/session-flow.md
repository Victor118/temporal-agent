# Session flow — séquence d'un message utilisateur

Diagramme de séquence d'un message adressé à deux agents (« @jarvis résume,
@smith juge ») : le serveur l'enregistre, le livre au participant de Jarvis,
qui le relaie à celui de Smith une fois son tour fini. Jarvis délègue à un
sous-agent qui pose une question à l'user. Une session n'a pas de workflow à
elle : chaque agent qui y répond est un participant (`ParticipantWorkflow`),
qui ne vit que le temps de vider sa boîte. Conception : `docs/design/participants.md`.

```mermaid
sequenceDiagram
    autonumber
    actor U as User (web/telegram)
    participant H as HTTP adapter + session.Service<br/>(cmd/agent, session)
    participant DB as Postgres
    participant T as Temporal
    participant PJ as ParticipantWorkflow<br/>(&lt;session&gt;:p:jarvis)
    participant GA as AgentWorkflow<br/>(&lt;session&gt;:p:jarvis:m10)
    participant LLM as CallLLM activity
    participant SA as AgentWorkflow<br/>(sous-agent)
    participant AU as AskUserWorkflow
    participant PS as ParticipantWorkflow<br/>(&lt;session&gt;:p:smith)
    participant SSE as SSE Hub

    U->>H: POST /sessions/:id/messages
    Note over H: résout les agents (mentions, agent_mode)<br/>plafond : 5 messages en attente chez Jarvis → 429
    H->>DB: AppendMessage → id 10
    H->>SSE: user_message (agents, queued_behind)
    H->>T: SignalWithStart(<session>:p:jarvis, "message",<br/>{message_id: 10, next: [smith], part})
    T-->>PJ: démarre (ou rejoint sa boîte)

    PJ->>DB: CheckTurn (session, turn_end de m10.jarvis ?, agent)
    PJ->>SSE: turn_started (m10.jarvis) — web
    PJ->>GA: ExecuteChildWorkflow(AgentWorkflow, clé m10.jarvis)

    Note over GA: boucle ReAct
    GA->>LLM: CallLLM(références : clé du tour, noms d'outils)
    Note over LLM: charge la conversation (règle de lecture),<br/>l'ordonne par ancre, le prompt et les outils
    LLM-->>GA: tool_calls = [agent_analyst]
    GA->>SA: ChildWorkflow <tour>:tool:agent_analyst:<call>
    SA->>LLM: CallLLM (conversation inline)
    LLM-->>SA: tool_calls = [ask_user]
    SA->>AU: ChildWorkflow <sous-agent>:tool:ask_user:<call>
    AU->>SSE: ask_user (session = avant le premier ':')
    U->>H: POST /sessions/:id/answer
    H->>T: SignalWorkflow(AskUserWorkflow, "user-answer")
    AU-->>SA: réponse
    SA-->>GA: réponse finale (string)
    GA->>LLM: CallLLM
    LLM-->>GA: réponse finale
    GA->>SSE: message (signé)
    GA-->>PJ: AgentWorkflowOutput

    PJ->>DB: PersistContext (réécriture idempotente)
    PJ->>DB: EndTurn → m10.jarvis:end
    PJ->>T: Relay (activity) : SignalWithStart(<session>:p:smith,<br/>{message_id: 10, earlier_turns: [m10.jarvis]})
    PJ->>SSE: turn_done (m10.jarvis)
    Note over PJ: boîte vide → fin du workflow
    T-->>PS: démarre ; tour m10.smith, qui lit le tour de Jarvis
```

## Points clés

- **Un participant par agent et par session**, `<session>:p:<agent>` : il
  traite ses messages dans l'ordre, un à la fois ; des participants différents
  travaillent en parallèle. Il se termine quand sa boîte est vide ; un signal
  arrivé pendant cette fin est gardé par le serveur Temporal
  (`UNHANDLED_COMMAND`), et le message suivant le redémarre par
  `SignalWithStart`. Remise à neuf (`continue-as-new`) avec sa boîte.
- **Le signal ne porte pas le texte** : le message est en base avant d'être
  livré, le tour le charge avec la conversation.
- **Tout identifiant de workflow d'une session commence par `<session>:`** :
  le tour `<participant>:m<id>`, ses outils `<tour>:tool:<outil>:<appel>`,
  ceux d'un sous-agent à la suite. `ask_user` et les runs Claude Code
  retrouvent la session avant le premier `:`.
- **Clé de tour** `m<id du message>.<participant>` : l'ancre (le message
  répondu) et le participant. Un tour lit les messages hors tour jusqu'à son
  ancre, son tour et ceux du relais entiers, ses tours ancrés avant, et ceux
  des autres participants terminés avant son ancre (`turn_end`). Jamais un tour
  d'un autre participant à moitié écrit.
- **Le participant tient le tour** : il vérifie en base l'agent et la
  déduplication, envoie `turn_started` / `turn_done`, réécrit le tour, écrit
  son `turn_end` dans tous les cas (succès, échec, arrêt), notifie une erreur,
  et relaie le message au participant suivant (activity, bornée ; échec final :
  `turn_end` d'erreur chez le destinataire).
- Côté UI, la ligne « … travaille… » nomme tous les participants au travail ;
  « Arrêter » envoie `stop-turn` à chacun. Chaque événement SSE porte un `id:` ;
  un client qui se reconnecte reçoit ceux qu'il a manqués, ou `reload`.
- Les sous-agents n'ont pas de mémoire propre : leur conversation est inline
  et meurt avec leur exécution.
