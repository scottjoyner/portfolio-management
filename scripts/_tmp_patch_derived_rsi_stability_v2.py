from pathlib import Path

source = Path("scripts/_tmp_patch_derived_rsi_stability.py").read_text(encoding="utf-8")
exec(compile(source, "<derived-rsi-stability-v2>", "exec"), globals(), globals())

path = Path("tests/test_canonical_replay.py")
path.write_text(path.read_text(encoding="utf-8").rstrip() + "\n", encoding="utf-8")
