# Durable Workflows

A small workflow engine built to learn durable execution. The Go service persists each run, its event history, and activity tasks in PostgreSQL. A worker claims tasks with leases; an expired lease lets another worker retry after a crash. The Next.js console shows runs and their timelines.

This first milestone supports one sequential example workflow (`text-pipeline`). It does not yet provide arbitrary Go workflow functions, replay, external activities, timers, signals, or authentication.

## Run locally

Requirements: Docker and Node.js 22+. The database uses the official PostgreSQL 17 image; Compose builds the Go service image from `backend/Dockerfile`.

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

Run the database integration test with Go 1.24+ installed:

```sh
cd backend
TEST_DATABASE_URL='postgres://workflow:workflow@localhost:5432/workflows?sslmode=disable' go test ./... -count=1 -v
```

## Durability contract

- Starting a run writes its initial event and first task in one transaction.
- Finishing an activity writes its result, updates the run, and creates the next task in one transaction.
- A lease token fences a worker whose claim has expired.
- External side effects will need idempotency keys when external activities are added. The engine cannot make an arbitrary network call exactly once.

## Next milestones

Add configurable activity handlers and explicit failure injection; add durable timers and signals; then implement deterministic workflow code replay and definition versioning.
