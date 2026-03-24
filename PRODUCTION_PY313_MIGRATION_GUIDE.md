# CatMapperAPI Production Migration Guide: Python 3.13 (uWSGI)

Date: 2026-03-24
Scope: Runtime/platform migration only (no API contract changes)

## 1. Target State
- API runtime image uses `python:3.13-slim-bookworm`.
- uWSGI is pinned to `2.0.31`.
- API container listens on uWSGI socket `:5000`.
- Nginx continues using `uwsgi_pass api:5000`.
- Worker containers (`api-worker`, `api-waiting-worker`) run the same rebuilt Python 3.13 image.
- Canary route/service has already been removed after validation.

## 2. Version Anchors
Use these as reference points that were validated in staging:
- Root app repo (`/mnt/storage/app`): commit `50e62e8` on branch `testserver`.
- API repo (`/mnt/storage/app/CatMapperAPI`): commit `d11fae4` on branch `main`.

Before migration, capture your current production commits:
```bash
cd /mnt/storage/app && git rev-parse --short HEAD
cd /mnt/storage/app/CatMapperAPI && git rev-parse --short HEAD
```

## 3. Pre-Maintenance Checklist
- Confirm maintenance window and rollback owner.
- Confirm backups for `config.ini`, `.env`, `docker-compose.yml`, and nginx config.
- Confirm Docker daemon healthy and disk space available.
- Confirm no uncommitted server-local edits.

Recommended snapshot:
```bash
ts=$(date +%Y%m%d-%H%M%S)
mkdir -p /mnt/storage/backups
cp /mnt/storage/app/docker-compose.yml /mnt/storage/backups/docker-compose.yml.$ts
cp /mnt/storage/app/conf/nginx.conf /mnt/storage/backups/nginx.conf.$ts
cp /mnt/storage/app/CatMapperAPI/config.ini /mnt/storage/backups/config.ini.$ts
cp /mnt/storage/app/CatMapperAPI/.env /mnt/storage/backups/api.env.$ts
```

## 4. Pull and Verify Code
```bash
cd /mnt/storage/app
git fetch origin
git checkout testserver
git pull --ff-only origin testserver

cd /mnt/storage/app/CatMapperAPI
git fetch origin
git checkout main
git pull --ff-only origin main
```

Sanity checks:
```bash
rg -n "python:3.13-slim-bookworm|uWSGI==2.0.31" /mnt/storage/app/CatMapperAPI/Dockerfile
rg -n "packaging>=24,<25|numpy==2.2.6|pandas==2.2.3" /mnt/storage/app/CatMapperAPI/requirements.txt
rg -n "5000:5000" /mnt/storage/app/docker-compose.yml
rg -n "from turtle import up" /mnt/storage/app/CatMapperAPI/CMroutes -S
```

Expected: last command returns no matches.

## 5. Build and Cutover
Option A (recommended, controlled):
```bash
cd /mnt/storage/app
docker compose -f docker-compose.yml build --pull api api-worker api-waiting-worker
docker compose -f docker-compose.yml up -d --no-deps api api-worker api-waiting-worker
docker restart nginx
```

Option B (scripted release):
```bash
cd /mnt/storage/app/CatMapperAPI
sudo ./deploy.sh
```
Note: `deploy.sh` updates `.env` `VERSION`, commits that change, tags, and pushes.

## 6. Post-Cutover Validation
Container/runtime checks:
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}' | rg '^(api|api-worker|api-waiting-worker|nginx)\b|^NAMES' --no-line-number
docker exec api python --version
docker exec api uwsgi --version
docker exec nginx nginx -t
```

HTTP smoke tests:
```bash
curl -ksS -o /tmp/api_health.out -w '%{http_code}\n' -H 'Host: test.catmapper.org' https://127.0.0.1/api/health && cat /tmp/api_health.out
curl -ksS -o /tmp/api_foci.out -w '%{http_code}\n' -H 'Host: test.catmapper.org' 'https://127.0.0.1/api/foci?database=sociomap' && head -c 300 /tmp/api_foci.out; echo
```

Expected:
- Health returns `200` with JSON status.
- `/api/foci` returns `200` with JSON payload.

Queue checks:
```bash
docker logs --tail 100 api-worker
docker logs --tail 100 api-waiting-worker
```

## 7. Rollback Plan
Rollback trigger examples:
- Sustained 5xx increase
- Worker queue stalls
- Critical auth/upload flow regressions

Rollback procedure:
1. Checkout the pre-migration commits captured in step 2.
2. Rebuild API and worker images.
3. Recreate API/workers and restart nginx.

Commands:
```bash
cd /mnt/storage/app
git checkout <PREVIOUS_ROOT_COMMIT>

cd /mnt/storage/app/CatMapperAPI
git checkout <PREVIOUS_API_COMMIT>

cd /mnt/storage/app
docker compose -f docker-compose.yml build api api-worker api-waiting-worker
docker compose -f docker-compose.yml up -d --no-deps api api-worker api-waiting-worker
docker restart nginx
```

## 8. 24-Hour Watch Items
- API 5xx rate
- p95 latency on `/api` routes
- RQ worker throughput and failures
- Repeated uWSGI worker crash/restart patterns

## 9. Known Gotcha
- If `from turtle import up` appears again in `CMroutes/dev_routes.py`, API startup will fail on slim images due to tkinter/Tk dependency.
