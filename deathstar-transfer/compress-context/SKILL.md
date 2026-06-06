---
name: compress-context
title: Context Compression (MacBook Air qwen3.5-0.8b)
description: Summarize large technical contexts into focused key points using MacBook Air endpoint
category: mlops
references:
  - subagent_endpoints.md
---

# Context Compression Skill (MacBook Air)

## Overview

Uses MacBook Air `http://100.85.64.117:1234/v1` with qwen3.5-0.8b model for context compression and summarization. Primary role is Stage 1 in the three-agent evaluation pipeline.

---

## Endpoint Details

| Property | Value |
|----------|-------|
| **Endpoint** | `http://100.85.64.117:1234/v1` |
| **Primary Model** | qwen3.5-0.8b (~24s load time) |
| **Secondary Model** | gemma-3-1b (~13s load time, for quick tasks) |

---

## Primary Use Cases

✅ DO use this skill for:
- Summarizing 8K+ token documents
- Extracting main concepts and action items
- Removing redundancy while maintaining fidelity
- Complex reasoning tasks requiring context retention
- Identifying key points from lengthy technical content

---

## When NOT to Use This Skill

❌ DON'T use this skill for:
- Simple queries (use Dell OptiPlex instead)
- Instant-response needs (<1s required)
- Tasks where you don't need comprehensive summarization

---

## Load Time Expectations

**First Run:** ~24 seconds (qwen3.5 model load time)  
**Subsequent Runs:** May be faster if model stays loaded in memory

---

## Trigger Conditions

Use this skill when:
- You have a large technical context to summarize
- You need complex reasoning with full document retention
- You're starting the multi-agent evaluation pipeline (Stage 1)
- Quality of summarization outweighs load time concerns

---

## Output Format

```markdown
# Summary Title

## Key Points
- Point 1
- Point 2
- Point 3

## Action Items
- [ ] Item 1
- [ ] Item 2

## Important Notes
- Note 1
```

---

## Related Skills

- `multi-agent-evaluation-pipeline` - Three-stage evaluation workflow
- `build-plan` - Follow-on plan generation (Stage 2)
- `evaluate-plan` - Plan evaluation and recommendations (Stage 3)