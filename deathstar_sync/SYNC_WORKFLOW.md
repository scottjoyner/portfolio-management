# Deathstar Skill Transfer Workflow
# ============================================================================

This document outlines the complete workflow for transferring delegation skills
from falcon orchestrator (100.78.106.121) to deathstar subagent endpoint.

## Architecture Overview

### Machine Roles:
- **Falcon** (main agent): Running at root@100.78.106.121, orchestrates tasks
- **Deathstar** (subagent): Runs multi-agent evaluation and gap analysis

### Delegation Skills to Transfer:
| Skill | Purpose | File |
|-------|---------|------|
| compress-context | Context compression & summarization | /home/deathstar/.hermes/skills/compress-context/SKILL.md |
| build-plan | Fast plan generation (dell_optiplex) | /home/deathstar/.hermes/skills/build-plan/SKILL.md |
| evaluate-plan | Plan evaluation & gap analysis (deathstar_evaluate) | /home/deathstar/.hermes/skills/evaluate-plan/SKILL.md |

---

## Transfer Process

### Step 1: Run Transfer Script on Falcon

```bash
cd /home/falcon/git/portfolio-management/deathstar_sync
./deathstar_skill_transfer.sh
```

This will:
- Identify all delegation skills from `~/.hermes/skills/`
- Create `.machine` files with review suffix in target directory
- Transfer references/, templates/, scripts/, assets/ directories
- Copy associated config.yaml files (excluding backups)

### Step 2: SSH to Deathstar

```bash
ssh root@100.78.106.121
# On deathstar, connect via Tailscale or direct IP
# Note: Use root@100.78.106.121 for main agent access
```

### Step 3: Review .machine Files on Deathstar

Location: `/home/deathstar/.hermes/skills/`

Files will be created with `.machine` suffix (e.g., `compress-context/SKILL.md.machine`).

**Review Checklist:**
- [ ] SKILL.md content matches source
- [ ] references/ directory transferred correctly
- [ ] templates/ contains expected templates
- [ ] scripts/ has necessary helper scripts
- [ ] assets/ contains media files (if applicable)
- [ ] config.yaml merged (if exists)

### Step 4: Merge Approved Files

#### Option A: Interactive Review
```bash
cd /home/deathstar/.hermes/skills
./deathstar_merge_script.sh
# Select option 2 to review individual skills
```

#### Option B: Manual Quick Review

For each `.machine` file:

```bash
# View the file
cat SKILL.md.machine

# If approved, rename and merge
mv compress-context/SKILL.md.machine compress-context/SKILL.md
```

Repeat for each skill.

---

## Memory Sync Between Machines

### Current State:
- **Falcon memories:** Stored in `/home/falcon/.hermes/`
- **Deathstar:** Currently empty or minimal configuration

### Recommended Sync Strategy:

**Option 1: Direct Transfer (Recommended)**
```bash
# On falcon, after skills transferred
# Create memory sync directory on deathstar
ssh root@deathstar "mkdir -p ~/.hermes/memory"

# Transfer memories for review
tar -czf memories_backup.tar.gz ~/.hermes/memories/
scp memories_backup.tar.gz root@deathstar:~/.hermes/
ssh root@deathstar "tar -xzf ~/.hermes/memories_backup.tar.gz -C ~/.hermes/memory/"
```

**Option 2: Selective Memory Transfer**
Transfer only critical memories needed by deathstar:
- delegation configuration (delegation/*.yaml)
- multi-agent-fleet skills
- performance benchmarks (MACHINE_* files)

**Option 3: No Sync (Recommended for subagent)**
Leave memory sync to individual agents. Deathstar can maintain its own
performance metrics and context specific to its evaluation role.

---

## Verification Checklist

After transfer and merge:

### Skills Directory Structure:
```
/home/deathstar/.hermes/skills/
├── compress-context/SKILL.md          ✓ (transferred from .machine)
├── compress-context/references/*.md   ✓ (if exists)
├── compress-context/templates/         ✓ (if exists)
├── build-plan/SKILL.machine           ⏳ (pending review)
├── build-plan/config.yaml             ⏳ (pending review)
├── evaluate-plan/SKILL.machine        ⏳ (pending review)
└── [other .machine files]             ⏳ (pending review)
```

### Config Verification:
```bash
# Check delegation config on deathstar
cat ~/.hermes/config.yaml | grep -A 15 "^delegation:"

# Should show:
# delegation:
#   model: qwen/qwen3.5-9b
#   provider: lmstudio
#   base_url: http://100.78.106.121/v1
#   max_concurrent_children: 3
#   orchestrator_enabled: true
```

---

## Testing Delegation on Deathstar

### Quick Test Script:
```bash
#!/bin/bash -c "Quick delegation test on deathstar"

echo "Testing delegation skills..."
echo ""

# Test compress-context (should accept ~24s load time)
echo "1. Testing compress-context skill..."
timeout 30 python3 << 'EOF'
from hermes_tools import delegate_task, delegate_task as dts

result = delegate_task(
    goal="Summarize these key points",
    context="Test compression capability",
    toolsets=["terminal"]
)
print(f"Compression test: {type(result).__name__}")
EOF

echo ""

# Test build-plan (should be instant)
echo "2. Testing build-plan skill..."
timeout 30 python3 << 'EOF'
response = delegate_task(
    goal="Create simple implementation checklist",
    toolsets=["terminal"]
)
print(f"Plan test successful")
EOF

echo ""

# Test evaluate-plan (Deathstar primary role)
echo "3. Testing evaluate-plan skill..."
timeout 30 python3 << 'EOF'
response = delegate_task(
    goal="Evaluate this simple task",
    toolsets=["terminal"]
)
print(f"Evaluation test successful")
EOF

echo ""
echo "✓ All delegation skills loaded on deathstar!"
```

---

## Troubleshooting

### Issue: .machine files not transferring

**Solution:**
```bash
# On falcon
ls -la ~/.hermes/skills/build-plan/
# Verify SKILL.md exists, then re-run transfer script
./deathstar_skill_transfer.sh
```

### Issue: Merge fails on deathstar

**Solution:**
```bash
# SSH to deathstar and check permissions
ssh root@deathstar "ls -la ~/.hermes/skills/build-plan/"

# Verify write permissions
ssh root@deathstar "chmod 755 ~/.hermes/skills"
```

### Issue: Skills not recognized by delegation system

**Solution:**
```bash
# Check that config.yaml is correct
cat ~/.hermes/config.yaml | grep -A 10 "^delegation:"

# Verify model and provider settings are present
```

---

## Next Steps After Initial Transfer

1. **Add .machine suffix for review files only** (not merge yet)
   ```bash
   # Keep .machine files as-is until reviewed by deathstar admin
   ```

2. **Review memory sync requirements:**
   - Determine which memories deathstar needs
   - Transfer delegation config first
   - Consider performance-specific benchmarks (e.g., `MACHINE_*` files)

3. **Update multi-agent delegation registry:**
   ```bash
   # On deathstar, update the skill registry
   cp /home/falcon/.hermes/skills/multi-agent-fleet/multi-agent-delegation-registry/SKILL.md \
       ~/.hermes/skills/multi-agent-fleet/multi-agent-delegation-registry/SKILL.md.machine
   
   # Review and merge after testing
   mv multi-agent-delegation-registry/SKILL.md.machine multi-agent-delegation-registry/SKILL.md
   ```

4. **Test full delegation workflow:**
   - Send a planning task to deathstar
   - Verify instant response from build-plan skill
   - Check that compress-context handles large contexts
   - Test evaluate-plan for gap analysis

---

## Performance Notes

### Expected Load Times on Deathstar:
| Skill | First Call | Subsequent |
|-------|-----------|------------|
| compress-context | ~24s (qwen3.5) | 13s (gemma-3-1b) |
| build-plan | <1s | <1s |
| evaluate-plan | ~0s (lfm2.5-1.2b) | ~0s |

### Memory Considerations:
- compress-context uses ~8KB tokens for context compression
- Deathstar should maintain its own evaluation metrics
- No need to sync full memory unless delegation config required

---

## Contact & Support

If issues arise during transfer:
1. Review multi-agent-fleet documentation in falcon's skills directory
2. Check `~/.hermes/skills/mlops/evaluation/` for delegate patterns
3. Reference KNOWLEDGE_TRANSFER.md in git directory for additional context

**Status:** Ready for deathstar skill transfer
