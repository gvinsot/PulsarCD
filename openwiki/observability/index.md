# Files

- [Error detection and notifications](errors-and-notifications.md) - How the recurring error detector scans ERROR logs, normalizes and clusters them, filters protocol noise and its own output, then notifies people and the LLM agent, and how the notifier sends rate-limited, secret-free messages to ntfy, webhooks and email.
- [Log collection and search](log-collection-and-search.md) - How agents collect container logs and metrics on each node and write them directly to VictoriaLogs, how records are organized by kind and stream fields, how searches and raw LogsQL are built safely and bounded, and what can be lost or visible to viewers.
