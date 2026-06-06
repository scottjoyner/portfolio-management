#!/bin/bash
# ============================================================================
# Deathstar Skill Transfer Script
# Transfers delegation skills from falcon to deathstar with .machine review suffix
# ============================================================================

set -euo pipefail

FALCON_HOME="/home/falcon/.hermes"
DEATHSTAR_HOME="/home/deathstar/.hermes"
SKILLS_DIR="skills"
SKIP_LIST="config.yaml config.yaml.bak env .env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Deathstar Skill Transfer ===${NC}"
echo "Source: $FALCON_HOME$SKILLS_DIR"
echo "Target: $DEATHSTAR_HOME$SKILLS_DIR"
echo ""

# Check if deathstar directory exists, create if needed
if [ ! -d "$DEATHSTAR_HOME" ]; then
    echo -e "${YELLOW}⚠️  Deathstar home directory does not exist. Creating...${NC}"
    mkdir -p "$DEATHSTAR_HOME"
fi

# Check if skills directory exists on deathstar, create if needed
if [ ! -d "$DEATHSTAR_HOME$SKILLS_DIR" ]; then
    echo -e "${YELLOW}⚠️  Deathstar skills directory does not exist. Creating...${NC}"
    mkdir -p "$DEATHSTAR_HOME$SKILLS_DIR"
fi

# Function to copy a skill with machine suffix for review
copy_skill() {
    local source_dir="$1"
    local target_dir="$2"
    
    echo ""
    echo "----------------------------------------"
    if [ -d "$source_dir" ]; then
        echo "${GREEN}✓ Copying: $source_dir${NC}"
        
        # Create target directory if it doesn't exist
        mkdir -p "$target_dir"
        
        # Copy the SKILL.md with .machine suffix
        if [ -f "$source_dir/SKILL.md" ]; then
            cp "$source_dir/SKILL.md" "$target_dir/SKILL.md.machine"
            echo -e "  → ${YELLOW}SKILL.md.machine${NC} (review pending)"
            
            # Copy references directory if exists
            if [ -d "$source_dir/references" ]; then
                cp -r "$source_dir/references" "$target_dir/"
                echo -e "  → ${GREEN}references/ transferred${NC}"
            fi
            
            # Copy templates directory if exists
            if [ -d "$source_dir/templates" ]; then
                cp -r "$source_dir/templates" "$target_dir/"
                echo -e "  → ${GREEN}templates/ transferred${NC}"
            fi
            
            # Copy scripts directory if exists
            if [ -d "$source_dir/scripts" ]; then
                cp -r "$source_dir/scripts" "$target_dir/"
                echo -e "  → ${GREEN}scripts/ transferred${NC}"
            fi
            
            # Copy assets directory if exists
            if [ -d "$source_dir/assets" ]; then
                cp -r "$source_dir/assets" "$target_dir/"
                echo -e "  → ${GREEN}assets/ transferred${NC}"
            fi
        else
            echo -e "${RED}✗ No SKILL.md found in $source_dir${NC}"
        fi
        
        # Copy config.yaml if exists (without backup files)
        if [ -f "$source_dir/config.yaml" ] && [[ ! "$(basename "$source_dir")" =~ "^config" ]]; then
            cp "$source_dir/config.yaml" "$target_dir/config.yaml.machine"
            echo -e "  → ${YELLOW}config.yaml.machine${NC} (review pending)"
        fi
        
    else
        echo -e "${RED}✗ Source directory does not exist: $source_dir${NC}"
    fi
}

# List of skills to transfer
echo -e "${GREEN}Skills identified for transfer:${NC}"
SKILLS=(
    "compress-context"
    "build-plan" 
    "evaluate-plan"
)

for skill in "${SKILLS[@]}"; do
    source_dir="$FALCON_HOME$SKILLS_DIR/$skill"
    target_dir="$DEATHSTAR_HOME$SKILLS_DIR/$skill"
    
    echo ""
    if [ -d "$source_dir" ]; then
        echo -e "${GREEN}✓ Found: $skill${NC}"
        
        # Create target directory
        mkdir -p "$target_dir"
        
        # Copy SKILL.md with review suffix
        if [ -f "$source_dir/SKILL.md" ]; then
            cp "$source_dir/SKILL.md" "$target_dir/SKILL.md.machine"
            echo -e "  → ${YELLOW}SKILL.md.machine (review pending)${NC}"
            
            # Copy references
            if [ -d "$source_dir/references" ]; then
                cp -r "$source_dir/references" "$target_dir/"
                echo -e "  → ${GREEN}references/ transferred${NC}"
            fi
            
            # Copy templates  
            if [ -d "$source_dir/templates" ]; then
                cp -r "$source_dir/templates" "$target_dir/"
                echo -e "  → ${GREEN}templates/ transferred${NC}"
            fi
            
            # Copy scripts
            if [ -d "$source_dir/scripts" ]; then
                cp -r "$source_dir/scripts" "$target_dir/"
                echo -e "  → ${GREEN}scripts/ transferred${NC}"
            fi
            
            # Copy assets
            if [ -d "$source_dir/assets" ]; then
                cp -r "$source_dir/assets" "$target_dir/"
                echo -e "  → ${GREEN}assets/ transferred${NC}"
            fi
            
            # Copy config.yaml (excluding backup files)
            for file in "$source_dir"/config.yaml*; do
                if [[ ! "$(basename "$file")" =~ "\.bak\." ]] && [[ "$(basename "$file")" != "config.yaml.bak" ]]; then
                    if [ -f "$file" ]; then
                        filename=$(basename "$file")
                        cp "$file" "$target_dir/${filename}.machine"
                        echo -e "  → ${YELLOW}config/${filename}.machine${NC} (review pending)"
                    fi
                fi
            done
        else
            echo -e "${RED}✗ No SKILL.md in $source_dir${NC}"
        fi
        
    else
        echo -e "${RED}✗ Not found: $skill${NC}"
    fi
done

echo ""
echo "=========================================="
echo -e "${GREEN}Skill transfer complete!${NC}"
echo "=========================================="
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. SSH to deathstar (ssh root@deathstar.local or use Tailscale)"
echo "2. Review .machine files in /home/deathstar/.hermes/skills"
echo "3. Merge approved files using:"
echo "   - mv file.machine filename          # Auto-approve"  
echo "   - Add to merge list for manual review"
echo ""
echo -e "${GREEN}All skills now available on falcon:${NC}"
for skill in "${SKILLS[@]}"; do
    echo "  → $skill/SKILL.md"
done
