# MacBook Air Endpoint (qwen3.5-0.8b)

## Endpoint Details

**Address:** `http://100.85.64.117:1234/v1`  
**Model:** qwen3.5-0.8b *(primary)* + gemma-3-1b *(secondary)*  
**Load Time:** ~24s (qwen), 13s (gemma)  
**Role:** Stage 1 - Context compression, summarization

---

## Model Specifications

### qwen3.5-0.8b (Primary Model)

**Best For:**
- Summarizing 8K+ token documents
- Extracting main concepts and action items
- Complex reasoning tasks

**Load Time:** ~24 seconds on first run  
**Subsequent Runs:** May be faster if model stays loaded

### gemma-3-1b (Secondary Model)

**Best For:** Quick compression tasks where speed outweighs maximum quality  
**Load Time:** ~13 seconds  
**Use Case:** When you need faster responses for brief contexts

---

## Session-Specific Notes

### Performance Observed in Testing:

**First Run Cycle Time:** ~24-29 seconds  
**Stage 1 Compression Time:** ~24s (dominated by model load)  
**Stage 2 & 3 Follow-up Stages:** <1s combined (instant)

---

## Related Documentation

- `/home/falcon/git/lms/compress-context/SKILL.md` - Main compression skill
- `/home/falcon/git/lms/multi-agent-evaluation-pipeline/SKILL.md` - Pipeline skill

---

## Last Updated

Session: 2026-06-02  
Status: ✅ Endpoint registered and operational