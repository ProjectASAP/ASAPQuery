#!/bin/bash
# Add a git worktree for an issue number (uses gh to resolve the branch)

cd_into=0
if [ "$1" = "-c" ] || [ "$1" = "--cd" ]; then
  cd_into=1
  shift
fi

if [ -z "$1" ]; then
  echo "Usage: gwt.sh [-c|--cd] <issue-number>"
  echo "Example: gwt.sh 97"
  echo "  -c, --cd   open a shell in the worktree afterwards"
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
worktree_dir=".agent/worktrees/$branch_name"

echo "Found branch: $branch_name"

if [ -d "$worktree_dir" ]; then
  echo "Worktree already exists at $worktree_dir"
else
  echo "Fetching from origin..."
  git fetch origin "$branch_name"

  echo "Creating worktree at $worktree_dir..."
  mkdir -p .agent/worktrees
  if git show-ref --verify --quiet "refs/heads/$branch_name"; then
    git worktree add "$worktree_dir" "$branch_name" || exit 1
  else
    git worktree add --track -b "$branch_name" "$worktree_dir" "origin/$branch_name" || exit 1
  fi
fi

# ponytail: a script can't cd its parent shell, so spawn a new one there; `exit` returns.
if [ "$cd_into" -eq 1 ]; then
  cd "$worktree_dir" && exec "${SHELL:-bash}"
fi
