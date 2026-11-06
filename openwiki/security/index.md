# Files

- [Authentication and access control](authentication-and-access.md) - How people sign in with Google or a break-glass account, how session tokens are revoked, how the central middleware protects routes by default using two roles, which other credentials exist for agents, the edge, webhooks and MCP, and how headers and log redaction protect credentials.
- [Edge security and usage analytics](edge-security-and-usage.md) - How PulsarCD turns Traefik and WAF log lines into access records, detects attacks and reports usage per stack from them, serves a blocklist that the edge polls as dynamic configuration, and automatically bans clients probing for secrets, with exemptions that prevent locking out administrators.
