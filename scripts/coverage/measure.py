import json, sys, subprocess, os
mod = sys.argv[1]; testpath = sys.argv[2]; datafile = sys.argv[3]
r = subprocess.run([".venv/bin/coverage","run","--branch","--source="+mod,"--data-file="+datafile,
                    "-m","pytest",testpath,"-q"], capture_output=True, text=True, timeout=150)
if not os.path.exists(datafile):
    print("RUN_FAIL", r.returncode); sys.exit(0)
jf = datafile + ".json"
subprocess.run([".venv/bin/coverage","json","-o",jf,"--data-file="+datafile], capture_output=True)
with open(jf) as f: d = json.load(f)
for fn, info in d.get("files", {}).items():
    if fn.endswith(".py") and mod.replace(".","/") in fn:
        s = info["summary"]
        nl = s["num_statements"]; cl = s["covered_lines"]
        nb = s.get("num_branches",0); cb = s.get("covered_branches",0)
        line_pct = 100.0*cl/nl if nl else 100.0
        br_pct = 100.0*cb/nb if nb else 100.0
        print(f"{os.path.basename(fn)} line={line_pct:.1f} branch={br_pct:.1f} missL={s['missing_lines']} missB={s.get('missing_branches',[])}")
