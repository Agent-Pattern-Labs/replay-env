# User History Replay Loop

This loop makes a production user's history replayable before a change reaches
staging.

```text
Objective -> World -> Probe -> Trace -> Judge -> Repair -> Memory -> Gate
```

The initial probe is intentionally narrow:

1. Export one subject user and their touched user graph from production.
2. Materialize the capsule into the app's local replay database.
3. Open the app frontend with local auth mapped to the subject when needed.
4. Verify critical private/public views and cross-subject interactions render
   without mutation or unsafe provider side effects.
5. Capture trace evidence and decide whether the change can move to staging.
