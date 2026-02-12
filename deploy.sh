#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

# Branch check: Ensure we are on main
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$CURRENT_BRANCH" != "main" ]; then
  echo "❌ Error: You are on branch '$CURRENT_BRANCH'. You must be on 'main' to deploy."
  exit 1
fi

# Pre-flight check: Ensure git directory is clean
if [ -n "$(git status --porcelain)" ]; then 
  echo "❌ Error: You have uncommitted changes. Please commit or stash them before deploying."
  exit 1
fi

# 1. Generate the version number (Year.Month.Day.HourMinute)
NEW_VERSION=$(date +%Y.%m.%d.%H%M)

echo "🚀 Starting deployment for version: $NEW_VERSION"

# 2. Write the version to the .env file for Flask/Docker
# This ensures the API knows its own version
echo "VERSION=$NEW_VERSION" > .env

# 3. Restart the Docker container
echo "Restarting API container..."
docker restart api

# 4. Git Tagging
# This creates a local tag and pushes it to your remote (e.g., GitHub/GitLab)
echo "Creating Git tag: v$NEW_VERSION"

# Add the .env change so the version record is committed
git add .env -f
git commit -m "Deploy version $NEW_VERSION"

# Create the tag
git tag -a "v$NEW_VERSION" -m "Deployment on $(date)"

# Push the commit and the tag to the server
git push origin main
git push origin "v$NEW_VERSION"

echo "✅ Deployment complete. System is now on v$NEW_VERSION and tagged in Git."