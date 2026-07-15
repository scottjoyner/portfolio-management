import json, sys
data = json.load(open(sys.argv[1]))
for f, info in sorted(data["files"].items()):
    if not f.startswith("event_markets/"): continue
    s = info["summary"]
    stmts = s["num_statements"]; miss = s["missing_lines"]
    line_pct = 100.0*(stmts-miss)/stmts if stmts else 100
    cov = s["covered_branches"]; mbr = s["missing_branches"]
    bcover = 100.0*cov/(cov+mbr) if (cov+mbr) else 100.0
    status = "PASS" if (line_pct>=90 and bcover>=90) else "GAP"
    miss_lines = info.get("missing_lines", [])
    print(f"{f:45s} line={line_pct:5.1f}% branch={bcover:5.1f}% stmts={stmts} miss={miss} -> {status} missing={miss_lines}")
