# Memory

## Rules

- Every escaped production bug becomes a replay scenario.
- Every support ticket with concrete user history becomes a probe candidate.
- Every production incident updates either the world model, exporter graph,
  trace capture, judge, or gate.
- Every excellent end-to-end replay becomes a golden trace.
- Every missing data edge becomes a manifest regression.

## Initial Lessons

- Production replay needs a subject-centered graph, not only a single account
  row.
- Apps with fixed local/dev auth identities need subject rewrite support at
  materialization time.
- Provider credentials should be stripped from replay by default; provider
  behavior should be replayed through receipts, attempts, mocks, or staging
  credentials.
