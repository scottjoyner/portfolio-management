# Deathstar Skills Transfer - Ready Summary
# ============================================================================

## What's Been Set Up

### Scripts Created:
1. **`deathstar_skill_transfer.sh`** - Transfers skills from falcon to deathstar
   - Creates `.machine` files with review suffix
   - Copies references/, templates/, scripts/, assets/ directories
   - Handles config.yaml and backup file filtering
   
2. **`deathstar_merge_script.sh`** - Reviews and merges .machine files on deathstar
   - Interactive review of all delegation skills
   - Automatic backup before merge
   - Clear processed files after successful merge

### Delegation Skills Identified:
| Skill | Location | Purpose |
|-------|----------|---------|
| compress-context | `~/.hermes/skills/compress-context` | Context compression for long contexts (~24s load) |
| build-plan | `~/.hermes/skills/build-plan` | Fast plan generation (<1s, instant) |
| evaluate-plan | `~/.hermes/skills/evaluate-plan` | Plan evaluation & gap analysis (Deathstar primary role) |

### Documentation:
- `SYNC_WORKFLOW.md` - Complete transfer process documentation
- `MACHINE_SUFFIX_PATTERN.md` - How to handle .machine files for review

---

## Next Steps

### Step 1: Transfer Skills to Deathstar

```bash
cd /home/falcon/git/portfolio-management/deathstar_sync
./deathstar_skill_transfer.sh
```

This creates `.machine` files in deathstar's skills directory.

### Step 2: SSH to Deathstar and Review

```bash
ssh root@100.78.106.121  # Main agent access via Tailscale or IP
cd /home/deathstar/.hermes/skills
```

Review each .machine file and merge using:

```bash
# Quick manual review (recommended)
cat compress-context/SKILL.md.machine
mv compress-context/SKILL.md.compress-context/SKILL.md

# Or use interactive script
./deathstar_merge_script.sh  # Select option 2 for specific skill
```

### Step 3: Verify Configuration on Deathstar

After transfer, deathstar should have:
- `delegation/` config with orchestrator settings (lfm2.5-1.2b model)
- All delegation skills available in `/home/deathstar/.hermes/skills/`
- Memory synced if needed (see SYNC_WORKFLOW.md for options)

---

## Current Status

### ✅ Completed:
- Transfer scripts created and tested
- Documentation written (SYNC_WORKFLOW.md)
- Multi-agent-fleet directory identified
- Memory sync pattern defined (.machine suffix on all machines)

### ⏳ Pending:
- Actual skill transfer to deathstar
- Review of .machine files on deathstar
- Merge approved files into production
- Memory sync if required for delegation config

---

## Quick Reference Commands

### From Falcon:
```bash
# Transfer skills
./deathstar_skill_transfer.sh

# View what will be transferred
ls -la ~/.hermes/skills/build-plan/
ls -la ~/.hermes/skills/compress-context/
ls -la ~/.hermes/skills/evaluate-plan/
```

### On Deathstar (after transfer):
```bash
# List .machine files waiting review
find /home/deathstar/.hermes/skills -name "*.machine"

# View compress-context for review
cat /home/deathstar/.hermes/skills/build-plan/SKILL.md.machine

# Merge all approved files
./deathstar_merge_script.sh  # Select option 2, then review each
```

---

## Memory Sync Notes

For memory sync between falcon and deathstar:

**Recommendation:** Sync delegation configuration and performance-specific files only.

### To sync memories (if needed):
```bash
# On falcon, after skills transferred
tar -czf memories_for_deathstar.tar.gz ~/.hermes/memories/delegation/
scp memories_for_deathstar.tar.gz root@deathstar:~/.hermes/memories_backup/

# On deathstar
tar -xzf memories_for_deathstar.tar.gz -C /home/deathstar/.hermes/memories/
```

**Alternative:** No memory sync needed if deathstar maintains its own evaluation metrics.

---

## Verification Checklist

After transfer complete:

- [ ] `.machine` files exist in `/home/deathstar/.hermes/skills/`
- [ ] compress-context/SKILL.md.machine ready for review
- [ ] build-plan/SKILL.md.machine ready for review
- [ ] evaluate-plan/SKILL.md.machine ready for review
- [ ] References, templates, scripts transferred (if exist)
- [ ] Config.yaml merged on deathstar with .machine suffix

---

## Important Notes

### About .machine Files:
- All transferred files have `.machine` suffix to mark them as "review pending"
- This pattern ensures both machines track review state
- Never merge without reviewing unless urgent (use `mv file.machine filename`)
- Always keep originals before merging

### Performance Expectations on Deathstar:
| Skill | Load Time | Best For |
|-------|-----------|----------|
| compress-context | ~24s first call | Long context compression, complex analysis |
| build-plan | <1s (instant) | Quick planning, checklists, breakdowns |
| evaluate-plan | ~0s | Gap analysis, recommendations, evaluation |

---

## Troubleshooting Quick Reference

### Issue: Transfer shows no files found
**Fix:** Verify source skills exist in `~/.hermes/skills/`

### Issue: Deathstar merge fails
**Fix:** Check permissions with `ls -la /home/deathstar/.hermes/skills/`

### Issue: Skills not recognized
**Fix:** Ensure delegation config merged in `/home/deathstar/.hermes/config.yaml`

---

## Documentation Files

All transfer-related docs located at:
```
/home/falcon/git/portfolio-management/deathstar_sync/
├── deathstar_skill_transfer.sh      # Main transfer script
├── deathstar_merge_script.sh        # Deathstar review & merge
└── SYNC_WORKFLOW.md                 # Complete documentation
```

---

**Status:** Transfer infrastructure ready. Run `./deathstar_skill_transfer.sh` to begin!
