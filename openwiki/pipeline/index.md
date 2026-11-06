# Files

- [Build, test and deploy pipeline](build-test-deploy.md) - How a commit on a starred repository becomes a deployed version, covering triggers, the per-repo lock, the build, test, QA and deploy chain with configurable gates, convergence checks, automatic rollback, what a backend restart does, and the persistent run history.
- [Preview environments](preview-environments.md) - How PulsarCD deploys a branch as an isolated stack on its own hostname, how names are derived safely, the protections that stop a preview touching production or QA, expiry, and what DNS, TLS and stateful dependencies a preview needs.
- [Project integration, stack env and secrets](project-integration-and-secrets.md) - What a repository needs in order to be deployed by PulsarCD, how environment variables become Docker secrets by naming convention, why changed secrets are not picked up automatically, how QA overrides, build secrets and encrypted backups of env files work, and how old tags are cleaned.
