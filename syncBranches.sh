#!/bin/bash

set -euo pipefail

# Exit on uncommitted changes
if [[ -n "$(git status --porcelain)" ]]; then
  echo "❌ Uncommitted changes found. Please commit or stash them first."
  exit 1
fi

# Ensure you're in a git repo
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "❌ Not inside a Git repository."
  exit 1
fi

# Fetch latest updates
echo "🔄 Fetching latest from remote..."
git fetch origin

# Sync from dev → main
echo "🚥 Checking out main..."
git checkout main

echo "⬇️ Pulling latest main..."
git pull origin main

echo "🔁 Merging dev into main..."
if ! git merge --no-ff dev -m "Merge dev into main (automated sync)"; then
  echo "❌ Merge conflict when merging dev into main. Resolve manually."
  exit 1
fi

echo "✅ Pushing merged main..."
git push origin main

# Sync from main → dev
echo "🚥 Checking out dev..."
git checkout dev

echo "⬇️ Pulling latest dev..."
git pull origin dev

echo "🔁 Merging main into dev..."
if ! git merge --no-ff main -m "Merge main into dev (automated sync)"; then
  echo "❌ Merge conflict when merging main into dev. Resolve manually."
  exit 1
fi

echo "✅ Pushing merged dev..."
git push origin dev

echo "🎉 Branches successfully synced."
