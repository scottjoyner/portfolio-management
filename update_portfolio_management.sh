#!/bin/bash
# Update script for portfolio-management repository
# Handles unrelated histories by merging them appropriately

set -e

cd /home/falcon/git/portfolio-management

echo "=== Portfolio Management Repository ==="
echo ""
echo "Current HEAD:"
git log -1 --oneline

echo ""
echo "Remote origin/main:"
git ls-remote --heads origin main | cut -f1

echo ""
echo "=== Checking status ==="
git status --porcelain | head -20

echo ""
echo "=== Updating portfolio-management repo ==="
echo "If you want to merge unrelated histories, run:"
echo "  git pull origin main --allow-unrelated-histories"
echo ""
echo "Or use rebase if they share a common ancestor:"
echo "  git rebase origin/main"
echo ""
echo "Current branch: $(git branch --show-current)"
