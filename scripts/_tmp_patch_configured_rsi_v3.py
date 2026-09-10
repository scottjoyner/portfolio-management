from pathlib import Path

source = Path("scripts/_tmp_patch_configured_rsi_v2.py").read_text(encoding="utf-8")
exec(compile(source, "<typed-rsi-patch-v3>", "exec"), globals(), globals())

path = Path("tests/test_canonical_replay.py")
text = path.read_text(encoding="utf-8").rstrip() + "\n"
path.write_text(text, encoding="utf-8")
