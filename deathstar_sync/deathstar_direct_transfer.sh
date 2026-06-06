#!/bin/bash
# Direct Deathstar Skill Transfer - Simplified Version
# Copies SKILL.md files directly with .machine suffix for review

set -euo pipefail

echo "=== Deathstar Skill Transfer (Direct Copy) ==="
echo ""

FALCON_SKILLS="/home/falcon/.hermes/skills"
DEATHSTAR_SKILLS="/home/deathstar/.hermes/skills"

# Create target directories
mkdir -p "$DEATHSTAR_SKILLS/build-plan"
mkdir -p "$DEATHSTAR_SKILLS/compress-context"
mkdir -p "$DEATHSTAR_SKILLS/evaluate-plan"

echo "Transferring delegation skills..."
echo ""

# Transfer build-plan (instant plan generation)
echo "1. build-plan skill..."
cp /home/falcon/.hermes/skills/build-plan/SKILL.md \
   "/home/deathstar/.hermes/skills/build-plan/SKILL.md.machine"
echo "   → SKILL.md.machine created on deathstar"

# Transfer compress-context (context compression)
echo "2. compress-context skill..."
cp /home/falcon/.hermes/skills/compress-context/SKILL.md \
   "/home/deathstar/.hermes/skills/compress-context/SKILL.md.machine"
echo "   → SKILL.md.machine created on deathstar"

# Transfer evaluate-plan (gap analysis - Deathstar primary role)
echo "3. evaluate-plan skill..."
cp /home/falcon/.hermes/skills/evaluate-plan/SKILL.md \
   "/home/deathstar/.hermes/skills/evaluate-plan/SKILL.md.machine"
echo "   → SKILL.md.machine created on deathstar"

# Transfer references directories
echo ""
echo "Transferring reference files..."

cp -r /home/falcon/.hermes/skills/build-plan/references \
     "/home/deathstar/.hermes/skills/build-plan/"
echo "   → build-plan/references/ transferred"

cp -r /home/falcon/.hermes/skills/compress-context/references \
     "/home/deathstar/.hermes/skills/compress-context/"
echo "   → compress-context/references/ transferred"

cp -r /home/falcon/.hermes/skills/evaluate-plan/references \
     "/home/deathstar/.hermes/skills/evaluate-plan/"
echo "   → evaluate-plan/references/ transferred"

echo ""
echo "=========================================="
echo "Skill transfer complete!"
echo "=========================================="
echo ""
echo "Files on deathstar for review:"
echo "  /home/deathstar/.hermes/skills/build-plan/SKILL.md.machine"
echo "  /home/deathstar/.hermes/skills/compress-context/SKILL.md.machine"
echo "  /home/deathstar/.hermes/skills/evaluate-plan/SKILL.md.machine"
echo ""
echo "Next step: SSH to deathstar and review .machine files."
