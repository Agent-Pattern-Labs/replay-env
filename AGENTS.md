# Replay Env Agent Notes

This repo is the pre-staging replay layer.

When adding app manifests or runners:

- Keep production export explicit and scoped to a named subject, such as a user,
  account, workspace, tenant, project, or organization.
- Do not read local `.env` files unless the user explicitly asks.
- Do not commit replay capsules, logs, secrets, or raw production exports.
- Default to safe redaction and make raw export an explicit flag.
- Preserve enough real product content to reproduce behavior, but strip
  credentials and external channel identifiers unless raw mode is required.
- Materialize into local or staging-like targets only.
- Prefer adding or updating a manifest under `config/apps/` over adding
  app-specific code paths.
- Treat every production incident as a future scenario and every fixed escaped
  bug as a regression replay.
