# Learning lab: the first durable workflow

The `text-pipeline` definition has three activities: uppercase, reverse, and SHA-256. Its output is stored in `workflow_runs.current_value`; every transition appears in `history_events`. This is a durable state machine, not yet a Temporal-style Go workflow SDK.

## Trace one run

1. Run `docker compose stop engine` and `docker compose up -d mysql`, then start only the API with `docker compose run --rm --service-ports -e ENGINE_MODE=api engine`. No worker is running yet.
2. Start a run from the Next.js console with `{"text":"hello"}`. It stays `running` because its first task is pending.
3. In MySQL, inspect `workflow_runs`, `history_events`, and `activity_tasks`. Check that the first event and task appeared together.
4. In another terminal, start a worker with `docker compose run --rm -e ENGINE_MODE=worker engine`. Refresh the run detail page. The three activity events and completion event appear in order.

## Crash and recover

1. Stop the worker. Start another with `docker compose run --rm -e ENGINE_MODE=worker -e WORKER_ACTIVITY_DELAY=10s engine`.
2. Start a new run. Wait for the `claimed run=...` log line, then terminate that worker before ten seconds pass.
3. Inspect its task: it is `leased`, and `lease_until` is in the future. The run is still `running`.
4. Start a normal worker with `docker compose run --rm -e ENGINE_MODE=worker engine`. After the 30-second lease expires, it reclaims the task and finishes the run. The history contains one completion event for each step.

When finished, stop the temporary API and worker containers with Ctrl-C, then restore the default service with `docker compose up -d engine`.

The claim may happen more than once, but only the worker holding the current lease token may commit a result. A real external activity can still execute twice if a process dies after making its network call. Its external API must accept a stable idempotency key.

## What to build next

1. Add an activity that intentionally fails twice, then succeeds. Observe attempts, backoff, and history.
2. Add a timer task that survives restarting every Go process.
3. Add a signal API that resumes a waiting run.
4. Implement deterministic workflow replay from event history, then add definition versioning.
