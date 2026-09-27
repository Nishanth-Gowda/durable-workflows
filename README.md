# Durable Workflows

A small workflow engine built to learn durable execution. The Go service persists each run, its event history, and activity tasks in MySQL. A worker claims tasks with leases; an expired lease lets another worker retry after a crash. The Next.js console shows runs and their timelines.

This first milestone supports one sequential example workflow (`text-pipeline`) and resetting a run from a saved checkpoint. It does not yet provide arbitrary Go workflow functions, deterministic code replay, external activities, timers, signals, or authentication.

## Run locally

Requirements: Docker and Node.js 22+. The database uses the official MySQL 8.4 image; Compose builds the Go service image from `backend/Dockerfile`.

```sh
docker compose up --build -d
docker compose ps
```

In a second terminal:

```sh
cd web
npm install
npm run dev
```

Open <http://localhost:3000>. To exercise the API directly:

```sh
curl -X POST http://localhost:8080/api/runs \
  -H 'Content-Type: application/json' \
  -d '{"workflow_name":"text-pipeline","input":{"text":"hello"}}'
```

Then request `GET /api/runs` or `GET /api/runs/{id}`. The engine serves the API and worker in one container by default. Use `docker compose logs -f engine` to follow task claims. Workers poll every 500 ms; leases expire after 30 seconds. Follow [LEARNING.md](LEARNING.md) for the crash experiment. `ENGINE_MODE=api` and `ENGINE_MODE=worker` can also run as separate containers or local Go processes.

The run detail page lists saved checkpoints. To reset through the API, first read `GET /api/runs/{id}` and choose a `checkpoints[].sequence`, then send:

```sh
curl -X POST http://localhost:8080/api/runs/RUN_ID/reset \
  -H 'Content-Type: application/json' \
  -d '{"checkpoint_sequence":2}'
```

The run resumes at that checkpoint's next activity. Its old events remain in the audit timeline, and the reset adds a `WorkflowReset` event. Workers holding tasks from before the reset cannot commit them.

Connect any MySQL client (such as MySQL Workbench or DBeaver) to host `127.0.0.1`, port `3306`, database `workflows`, username `workflow`, and password `workflow`. In the terminal, open a SQL prompt with:

```sh
docker compose exec mysql mysql -uworkflow -p workflows
```

Enter `workflow` when prompted. Try `SELECT * FROM workflow_runs;`, `SELECT * FROM history_events ORDER BY occurred_at DESC LIMIT 10;`, and `SELECT * FROM activity_tasks;`. These credentials are for local development only.

Run the database integration test with Go 1.24+ installed. Create an isolated test database first:

```sh
docker compose exec mysql mysql -uroot -p -e 'CREATE DATABASE IF NOT EXISTS workflows_test; GRANT ALL PRIVILEGES ON workflows_test.* TO "workflow"@"%";'
cd backend
TEST_MYSQL_DSN='workflow:workflow@tcp(127.0.0.1:3306)/workflows_test?parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27' go test ./... -count=1 -v
```

The previous PostgreSQL Docker volume remains available for recovery, but its runs are not copied into the new MySQL database. Do not run `docker compose down -v` if you need that volume.

## Durability contract

- Starting a run writes its initial event and first task in one transaction.
- Finishing an activity writes its result, updates the run, and creates the next task in one transaction.
- A lease token fences a worker whose claim has expired.
- Resetting a run restores the value saved by a selected history checkpoint and replaces its task in one transaction.
- External side effects will need idempotency keys when external activities are added. The engine cannot make an arbitrary network call exactly once.

## Next milestones

Add configurable activity handlers and explicit failure injection; add durable timers and signals; then implement deterministic workflow code replay and definition versioning.
