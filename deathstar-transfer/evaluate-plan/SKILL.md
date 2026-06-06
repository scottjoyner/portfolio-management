---
name: evaluate-plan
title: Plan Evaluation (Deathstar lfm2.5-8b-a1b multi-agent) - Highest Throughput Endpoint
description: Comprehensive gap analysis, feasibility assessment, and recommendations using Deathstar endpoint - instant response when loaded. Stage 3 in three-agent pipeline. **Highest throughput at 74 tokens/sec!**
category: mlops
version: 1.2.0 (IPv4 routing fix + performance benchmarks)
author: User Fleet Infrastructure
references:
  - references/deathstar_lfm2.5_multi_agent.md
  - references/optiplex_benchmark_results.txt
---

# Plan Evaluation Skill (Deathstar Multi-Agent Endpoint)

## Overview

Uses Deathstar `http://100.78.106.121:1234/v1` with lfm2.5-8b-a1b multi-agent model for comprehensive gap analysis, feasibility assessment, and recommendations. Primary role is Stage 3 in the three-agent evaluation pipeline - responds instantly since model is already loaded.

**Performance Benchmark (June 2026):**
- ✅ **Throughput**: 74 tokens/sec ⭐ **HIGHEST OF ALL THREE AGENTS!**
- ✅ **Response Time**: <100ms (instant) when loaded
- ✅ **Load Time**: ~3.66s time-to-first-token, ~0s when pre-loaded
- ✅ **Multi-Agent Coordination**: Can orchestrate three-subagent workflow

---

## ⚠️ CRITICAL: Endpoint Address Configuration

**Operational IPv4 Endpoint:** `http://100.78.106.121:1234/v1` ✅

### Why This Matters

Many fleet machines have **multiple network addresses**:
- ✅ **IPv4 Operational**: `http://100.78.106.121:1234/v1` (WORKING)
- ❌ **IPv6 Documentation**: `[2605:3c40::1f9d:18e9]:11395/v1` (No route to host)

**Common Error Pattern**: Delegating with IPv6 documentation prefix causes "No route to host" errors on most WSL deployments.

### Diagnosis and Fix Pattern

```python
# WRONG - Fails with "No route to host":
delegate_task(
    goal="evaluate this plan",
    endpoint="deathstar"  # [2605:3c40::1f9d:18e9] ❌ IPv6 documentation prefix!
)

# CORRECT - Verify operational IPv4 address first:
import requests

# Test connectivity before delegation:
response = requests.get(
    "http://100.78.106.121:1234/v1/health"  # ✅ Returns HTTP 200
)

# Now delegate with correct endpoint:
delegate_task(
    goal="evaluate this plan",
    endpoint="deathstar"  # Now works with IPv4 address!
)
```

### Fleet Deployment Note

Deathstar exists at **both** addresses:
1. ✅ **IPv4 operational**: `http://100.78.106.121:1234` - Primary, always use this first
2. ⚠️ **IPv6 Tailscale mesh**: `[2605:3c40::1f9d:18e9]` - Secondary, requires proper routing

**Always try IPv4 first!** IPv6 documentation prefix often has no route to host.

---

## Primary Use Cases

✅ DO use this skill for:
- Comprehensive gap analysis on implementation plans
- Multi-agent workflow coordination assessment
- Feasibility evaluation of technical approaches
- Performance implications analysis
- Reviewing implementation plans for missing components
- Prioritizing recommendations by impact/effort
- Strategic task decomposition
- Quality assurance before production deployment

**Best for**: Critical review tasks, feasibility gates, multi-perspective analysis.

---

## When NOT to Use This Skill

❌ DON'T use this skill for:
- Simple summary tasks (use compress-context instead)
- Quick plan creation without critical review (use build-plan instead)
- Tasks requiring 8K+ token summarization (use MacBook Air instead)
- Instant-response-only scenarios where <1s required (use Dell OptiPlex)

---

## Performance Characteristics

**Throughput**: 74 tokens/sec ⭐ **FASTEST ENDPOINT IN FLEET!**  
**Load Time**: ~0s ✅ Model already loaded at endpoint  
**Response Time**: <100ms (instant when loaded)  
**Time-to-First-Token**: ~3.66s (acceptable for gap analysis tasks)  

**Speed Ranking in Fleet Evaluation**: 
1st: Deathstar (74 t/s) ⭐ > 2nd: Dell OptiPlex (62.7 t/s) > 3rd: MacBook Air (24.5 t/s)

---

## Three-Agent Pipeline Role

### Stage 3: Plan Evaluation & Recommendations
```
[Raw Context] 
    ↓ [Compress Summary] (MacBook Air - ~24s load) ← Stage 1
[Compressed Summary]
    ↓ [Build Plan] (Dell OptiPlex - instant) ← Stage 2
[Implementation Plan]
    ↓ [Evaluate & Recommend] (Deathstar - instant) ← Stage 3 ✅
[Final Recommendations with Gap Analysis] ← Results to You
```

**Pipeline Contribution**: Deathstar provides comprehensive gap analysis and prioritized recommendations. Zero-latency response enables quick iteration after MacBook Air load time (~24s first call is the main bottleneck, not Deathstar).

---

## Trigger Conditions

Use this skill when:
- ✅ You have a plan to evaluate from build-plan (Stage 2)
- ✅ You need comprehensive gap analysis before implementation
- ✅ You want feasibility assessment with multi-perspective review
- ✅ You're completing multi-agent evaluation pipeline
- ✅ Critical decisions require thorough evaluation (A+/Grade level review)

---

## Usage Pattern: Full Three-Subagent Evaluation

### Example Workflow:

```python
# Stage 1: Compress Context (MacBook Air - ~24s load, high quality)
delegate_task(
    goal="Summarize technical context into focused key points",
    toolsets=["terminal"],
    endpoint="macbook_air"  # qwen3.5-0.8b for excellent quality
)

# Stage 2: Build Plan (Dell OptiPlex - instant response)
delegate_task(
    goal="Create implementation plan from compressed summary",
    toolsets=["terminal", "file"],
    endpoint="dell_optiplex"  # lfm2.5-1.2b for fast planning
)

# Stage 3: Evaluate Plan (Deathstar - instant gap analysis) ⭐ HIGHEST THROUGHPUT
delegate_task(
    goal="Review implementation plan and identify gaps/recommendations",
    toolsets=["terminal"],
    endpoint="deathstar"  # lfm2.5-8b-a1b for comprehensive evaluation
)
```

---

## Output Format

### Comprehensive Evaluation Report:

```markdown
# Plan Evaluation Report: [Title]

## Overall Assessment
- ✅ Ready to implement / ⚠️ Needs modifications / ❌ Major gaps identified

## Strengths
- Strength 1 description
- Strength 2 description

## Identified Gaps
1. **Gap Description** - Impact assessment
   - Recommended fix approach

2. **Gap Description** - Impact assessment  
   - Recommended fix approach

## Recommendations
### Priority: High (Blocker Issues)
- Recommendation 1 with implementation steps

### Priority: Medium (Should Address)
- Recommendation 2 with implementation steps

### Priority: Low (Nice to Have)
- Recommendation 3 with optional actions
```

---

## Related Skills

- `multi-agent-evaluation-pipeline` - Three-stage evaluation workflow  
- `build-plan` - Previous stage (plan generation, Stage 2)  
- `compress-context` - First stage (context compression, Stage 1)  

**Fleet Performance Note**: Deathstar provides the highest throughput (74 t/s) for comprehensive gap analysis and recommendations, making it ideal for critical review tasks in the multi-agent evaluation pipeline.

---

## Status: Operational and Benchmarked (June 2026)