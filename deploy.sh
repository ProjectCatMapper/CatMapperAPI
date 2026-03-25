#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

DEPLOY_USER="rjbischo"
APP_DIR="/mnt/storage/app/CatMapperAPI"
STACK_DIR="/mnt/storage/app"
COMPOSE_FILE="$STACK_DIR/docker-compose.yml"
ENV_FILE="$APP_DIR/.env"

if [ ! -d "$APP_DIR" ]; then
  echo "❌ Error: App directory not found: $APP_DIR"
  exit 1
fi

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "❌ Error: docker compose file not found: $COMPOSE_FILE"
  exit 1
fi

cd "$APP_DIR"

if [ ! -f "$ENV_FILE" ]; then
  echo "❌ Error: Missing required environment file: $ENV_FILE"
  echo "Deployment halted to avoid silently regenerating auth settings."
  echo "Restore the server-specific .env file and rerun deploy."
  exit 1
fi

# Require running as deploy user so git identity and permissions stay predictable.
if [ "$(id -un)" != "$DEPLOY_USER" ]; then
  echo "❌ Error: This script must be run as '$DEPLOY_USER' (without sudo)."
  echo "Run as that user and execute: ./deploy.sh"
  exit 1
fi

if ! id "$DEPLOY_USER" >/dev/null 2>&1; then
  echo "❌ Error: Deploy user '$DEPLOY_USER' does not exist on this system."
  exit 1
fi

run_as_deploy_user() {
  "$@"
}

# Detect current branch for push target
CURRENT_BRANCH=$(run_as_deploy_user git rev-parse --abbrev-ref HEAD)

# Pre-flight check: Ensure git directory is clean
if [ -n "$(run_as_deploy_user git status --porcelain)" ]; then 
  echo "❌ Error: You have uncommitted changes. Please commit or stash them before deploying."
  exit 1
fi

# 1. Generate the version number (Year.Month.Day.HourMinute)
NEW_VERSION=$(date +%Y.%m.%d.%H%M)

echo "🚀 Starting deployment for version: $NEW_VERSION"

# 2. Update VERSION in .env without clobbering other secrets.
run_as_deploy_user sed -i '/^VERSION=/d' .env
run_as_deploy_user bash -c "echo VERSION=$NEW_VERSION >> .env"

# 3. Ensure auth secret exists for signed API auth tokens.
if ! run_as_deploy_user grep -q '^CATMAPPER_AUTH_SECRET=' .env; then
  AUTH_SECRET=$(openssl rand -hex 64)
  run_as_deploy_user bash -c "echo CATMAPPER_AUTH_SECRET=$AUTH_SECRET >> .env"
  echo "Generated CATMAPPER_AUTH_SECRET in .env"
fi

# 4. Restart API, workers, and nginx so upstreams and job code are refreshed.
echo "Rebuilding API images (api/workers)..."
docker compose -f "$COMPOSE_FILE" build api api-worker api-waiting-worker

echo "Recreating API and worker containers..."
docker compose -f "$COMPOSE_FILE" up -d --no-deps api api-worker api-waiting-worker

echo "Restarting nginx container..."
docker restart nginx

# 5. Git Tagging
# Tag and push code only. Never commit secret-bearing environment files.
echo "Creating Git tag: v$NEW_VERSION"

if run_as_deploy_user git rev-parse "v$NEW_VERSION" >/dev/null 2>&1; then
  echo "❌ Error: Tag v$NEW_VERSION already exists."
  exit 1
fi

run_as_deploy_user git tag -a "v$NEW_VERSION" -m "Deployment on $(date)"

# Push branch tip and tag to remote.
run_as_deploy_user git push origin "$CURRENT_BRANCH"
run_as_deploy_user git push origin "v$NEW_VERSION"

echo "✅ Deployment complete. System is now on v$NEW_VERSION and tagged in Git."
