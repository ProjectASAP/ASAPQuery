#!/bin/bash
# Add a git worktree for a branch

if [ -z "$1" ]; then
  echo "Usage: gwt.sh <branch-name>"
  echo "Example: gwt.sh 97-add-option-to-record-prometheus-scrape-duration"
  exit 1
fi

branch_name="$1"
issue_num="${branch_name%%-*}"  # Extract issue number (everything before first hyphen)

echo "Fetching branch $branch_name from origin..."
git fetch origin "$branch_name"

echo "Creating worktree at ../worktrees/$issue_num for branch $branch_name..."
git worktree add --track -b "$branch_name" "../worktrees/$issue_num" "origin/$branch_name"
