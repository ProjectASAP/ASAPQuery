#!/bin/bash
# Add a git worktree for an issue number (uses gh to resolve the branch)

if [ -z "$1" ]; then
  echo "Usage: gwt.sh <issue-number>"
  echo "Example: gwt.sh 97"
  exit 1
fi

issue_num="$1"

branches=$(gh issue develop "$issue_num" --list 2>/dev/null | awk '{print $1}')

if [ -z "$branches" ]; then
  echo "No branch found for issue #$issue_num. Creating one via gh..."
  gh issue develop "$issue_num"
  branches=$(gh issue develop "$issue_num" --list 2>/dev/null | awk '{print $1}')
  if [ -z "$branches" ]; then
    echo "Failed to create branch for issue #$issue_num."
    exit 1
  fi
fi

branch_count=$(echo "$branches" | wc -l)

if [ "$branch_count" -gt 1 ]; then
  echo "Multiple branches found for issue #$issue_num:"
  echo "$branches"
  exit 1
fi

branch_name="$branches"

echo "Found branch: $branch_name"
echo "Fetching from origin..."
git fetch origin "$branch_name"

echo "Creating worktree at ../worktrees/$issue_num..."
if git show-ref --verify --quiet "refs/heads/$branch_name"; then
  git worktree add "../worktrees/$issue_num" "$branch_name"
else
  git worktree add --track -b "$branch_name" "../worktrees/$issue_num" "origin/$branch_name"
fi
