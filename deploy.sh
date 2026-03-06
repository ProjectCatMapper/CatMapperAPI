#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

DEPLOY_USER="rjbischo"
APP_DIR="/mnt/storage/app/CatMapperAPI"

if [ ! -d "$APP_DIR" ]; then
  echo "❌ Error: App directory not found: $APP_DIR"
  exit 1
fi

cd "$APP_DIR"

# Require sudo/root so deployment behavior is explicit.
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: This script must be run with sudo."
  echo "Run: sudo ./deploy.sh"
  exit 1
fi

if ! id "$DEPLOY_USER" >/dev/null 2>&1; then
  echo "❌ Error: Deploy user '$DEPLOY_USER' does not exist on this system."
  exit 1
fi

run_as_deploy_user() {
  sudo -u "$DEPLOY_USER" -H "$@"
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

# 2. Ensure .env exists and update VERSION without clobbering other secrets.
run_as_deploy_user touch .env
run_as_deploy_user sed -i '/^VERSION=/d' .env
run_as_deploy_user bash -c "echo VERSION=$NEW_VERSION >> .env"

# 3. Ensure auth secret exists for signed API auth tokens.
if ! run_as_deploy_user grep -q '^CATMAPPER_AUTH_SECRET=' .env; then
  AUTH_SECRET=$(openssl rand -hex 64)
  run_as_deploy_user bash -c "echo CATMAPPER_AUTH_SECRET=$AUTH_SECRET >> .env"
  echo "Generated CATMAPPER_AUTH_SECRET in .env"
fi

# 4. Restart API and worker containers so background jobs run updated code.
echo "Restarting API and worker containers..."
docker restart api
docker restart api-worker
if docker ps -a --format '{{.Names}}' | grep -qx "api-waiting-worker"; then
  docker restart api-waiting-worker
fi

# 5. Git Tagging
# This creates a local tag and pushes it to your remote (e.g., GitHub/GitLab)
echo "Creating Git tag: v$NEW_VERSION"

# Add the .env change so the version record is committed
run_as_deploy_user git add .env -f
run_as_deploy_user git commit -m "Deploy version $NEW_VERSION"

# Create the tag
run_as_deploy_user git tag -a "v$NEW_VERSION" -m "Deployment on $(date)"

# Push the commit and the tag to the server
run_as_deploy_user git push origin "$CURRENT_BRANCH"
run_as_deploy_user git push origin "v$NEW_VERSION"

echo "✅ Deployment complete. System is now on v$NEW_VERSION and tagged in Git."
