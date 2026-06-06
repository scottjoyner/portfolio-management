#!/bin/bash
# ============================================================================
# Deathstar Skill Merge Script
# Reviews .machine files and prepares them for merging into production
# ============================================================================

set -euo pipefail

SKILLS_DIR="/home/deathstar/.hermes/skills"
BACKUP_DIR="${SKILLS_DIR}/.review_backup/$(date +%Y%m%d_%H%M%S)"

echo "=========================================="
echo "Deathstar Skill Review & Merge Utility"
echo "=========================================="
echo ""

# Create backup directory if needed
mkdir -p "$BACKUP_DIR"

echo -e "${GREEN}Skills requiring review:${NC}"
find "$SKILLS_DIR" -name "*.machine" -type f | sort
echo ""

echo "Options:"
echo "1. Review and merge ALL .machine files"
echo "2. Review specific skill (e.g., compress-context.machine)"
echo "3. Backup review directory: $BACKUP_DIR"
echo "4. Clear all reviewed files after merge"
echo ""
read -p "Enter option [1-4]: " choice

case "$choice" in
    1)
        echo ""
        echo "Reviewing ALL .machine files..."
        echo "----------------------------------------"
        for file in $(find "$SKILLS_DIR" -name "*.machine" -type f | sort); do
            filename=$(basename "$file" .machine)
            dirpath=$(dirname "$file")
            skill_name=$(basename "$dirpath")
            
            echo ""
            echo "File: $skill_name/SKILL.md"
            echo "----------------------------------------"
            
            # Show first 20 lines for review
            echo "Preview (first 20 lines):"
            head -20 "$file" | cat
            
            echo ""
            read -p "Approve this merge? [y/N]: " approve
            
            if [[ "$approve" =~ ^[Yy]$ ]]; then
                # Remove .machine suffix and replace with original
                target="${dirpath}/${filename}"
                
                if [ -d "$target" ]; then
                    echo -e "${GREEN}✓ Merging $target${NC}"
                    mkdir -p "$dirpath" 2>/dev/null || true
                    mv "$file" "$target"
                    
                    # Also merge references, templates, etc.
                    for pattern in references templates scripts assets config*; do
                        pattern_file="${dirpath}/${pattern}.machine"
                        if [ -d "$pattern_file" ]; then
                            echo -e "  → ${GREEN}Merging: $pattern${NC}"
                            mv "$pattern_file" "${dirpath}/${pattern}"
                        elif [[ "$(basename "$pattern_file")" == *"config"* ]]; then
                            echo -e "  → ${GREEN}Merging config file: $pattern${NC}"
                            mv "$pattern_file" "${dirpath}/$(basename "$pattern_file" .machine)"
                        fi
                    done
                    
                    # Remove backup files (.bak.*) from target
                    find "${dirpath}" -name "*.bak.*" -type f -exec rm {} \; 2>/dev/null || true
                else
                    echo -e "${RED}✗ Target directory doesn't exist: $target${NC}"
                fi
                
            else
                echo "Skipping this file..."
            fi
        done
        
        # Remove all .machine files after successful merge
        echo ""
        echo "Removing processed .machine files..."
        find "$SKILLS_DIR" -name "*.machine" -type f -exec rm {} \; 2>/dev/null || true
        echo -e "${GREEN}✓ All .machine files removed${NC}"
        
    ;;
    
    2)
        echo "Enter skill name (e.g., compress-context):"
        read -p "> " skill_name
        
        file="$SKILLS_DIR/$skill_name.machine"
        
        if [ -f "$file" ]; then
            echo ""
            echo "Reviewing: $skill_name"
            echo "----------------------------------------"
            head -20 "$file" | cat
            
            echo ""
            read -p "Approve this merge? [y/N]: " approve
            
            if [[ "$approve" =~ ^[Yy]$ ]]; then
                target="${dirpath}/${filename}"
                mv "$file" "$target"
                
                # Merge associated files
                for pattern in references templates scripts assets config*; do
                    pattern_file="$dirpath/${pattern}.machine"
                    if [ -d "$pattern_file" ]; then
                        mv "$pattern_file" "${dirpath}/${pattern}"
                        echo -e "  → ${GREEN}Merged: $pattern${NC}"
                    elif [[ "$(basename "$pattern_file")" == *"config"* ]]; then
                        mv "$pattern_file" "${dirpath}/$(basename "$pattern_file" .machine)"
                        echo -e "  → ${GREEN}Merged config file${NC}"
                    fi
                done
                
                # Remove backup files from target
                find "$(dirname "$file")" -name "*.bak.*" -type f -exec rm {} \; 2>/dev/null || true
            fi
        else
            echo "File not found: $file"
        fi
        
    ;;
    
    3)
        echo -e "${GREEN}Backing up review directory to:${NC}"
        echo "$BACKUP_DIR"
        find "$SKILLS_DIR" -name "*.machine" -exec cp {} "$BACKUP_DIR/" \;
        
    ;;
    
    4)
        echo "Clearing all .machine files..."
        find "$SKILLS_DIR" -name "*.machine" -type f -exec rm {} \;
        echo -e "${GREEN}✓ All reviewed files cleared${NC}"
        
    ;;
esac

echo ""
echo "=========================================="
echo -e "${GREEN}Review session complete!${NC}"
echo "=========================================="
echo ""
echo "Current status:"
echo "  Remaining .machine files: $(find "$SKILLS_DIR" -name "*.machine" -type f | wc -l)"
echo ""
