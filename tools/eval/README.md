# Run simulation (e.g. 1000 jobs)

## 1. Start the cluster

From the repo root:

```bash
cd /path/to/cluster-docker
docker compose up -d
```

Wait until the edge API is up (e.g. 10–20 seconds), then check:

```bash
curl -s http://localhost:8081/api/jobs/available
```

## 2. (Optional) Choose scheduler mode

Set the algorithm before workers start claiming jobs. Edit `docker-compose.yml` under `edge-server` → `environment`:

```yaml
environment:
  FILE_STORAGE_DIR: /app/files
  SCHEDULER_MODE: PSO_ACO_GA   # or ACO_GA | Greedy | Round_Robin
```

Or override when starting:

```bash
SCHEDULER_MODE=Greedy docker compose up -d
```

Then run the simulation (step 3). To compare algorithms, change `SCHEDULER_MODE` and run again (restart edge-server so it picks up the new mode).

## 3. Run 1000-job simulation

From the repo root (so `tools/eval/run_1000.sh` is valid):

```bash
JOBS=1000 API_BASE=http://localhost:8081 ./tools/eval/run_1000.sh
```

- **JOBS**: number of jobs to submit (default 100). Use `JOBS=1000` for 1000 jobs.
- **API_BASE**: edge API URL. Use `http://localhost:8081` when calling from the host; inside Docker use `http://edge-server:8081`.
- **SEED**: random seed for reproducible task types/durations (default 42).

The script will:

1. Submit all jobs via `POST /api/jobs`.
2. Poll `GET /api/eval/report` every 2 seconds until all jobs are completed/failed/stopped.
3. Print the final eval report (throughput, latency, Gini, energy, etc.).

## 4. Save the report

To save the report to a file:

```bash
JOBS=1000 API_BASE=http://localhost:8081 ./tools/eval/run_1000.sh 2>&1 | tee run_1000.log
```

Then extract the JSON (e.g. the last `{ ... }` block) or call the API after the run:

```bash
curl -s http://localhost:8081/api/eval/report | jq .
```

## 5. Compare schedulers (example)

For a clean comparison, start with a fresh cluster so job history is empty (optional):

```bash
docker compose down
SCHEDULER_MODE=PSO_ACO_GA docker compose up -d
# wait, then JOBS=1000 ...
```

```bash
# PSO_ACO_GA (default)
docker compose up -d
JOBS=1000 API_BASE=http://localhost:8081 ./tools/eval/run_1000.sh 2>&1 | tee report_pso_aco_ga.txt

# Greedy: restart edge with new mode, then run again
SCHEDULER_MODE=Greedy docker compose up -d edge-server
sleep 5
JOBS=1000 API_BASE=http://localhost:8081 ./tools/eval/run_1000.sh 2>&1 | tee report_greedy.txt

# Round_Robin
SCHEDULER_MODE=Round_Robin docker compose up -d edge-server
sleep 5
JOBS=1000 API_BASE=http://localhost:8081 ./tools/eval/run_1000.sh 2>&1 | tee report_rr.txt
```

Compare `report_*.txt` (or the printed JSON) for `scheduler_mode`, `throughput_jobs_per_sec`, `gini_assignments`, `total_energy_joules`, etc.
