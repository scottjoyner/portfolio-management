from pathlib import Path

source = Path("scripts/_tmp_patch_configured_rsi.py").read_text(encoding="utf-8")
source = source.replace(
    '    end = text.index(end_marker, start)\n',
    '    try:\n        end = text.index(end_marker, start)\n    except ValueError:\n        if function_marker == "def build_alpha_evidence_from_canonical_replay(\\n":\n            end = len(text)\n        else:\n            raise\n',
    1,
)
source = source.replace(
    '    "\\n\\ndef verify_replay_attestation(",\n',
    '    "\\n\\ndef _basic_attestation_reasons(",\n',
)
exec(compile(source, "<typed-rsi-patch-v2>", "exec"), globals(), globals())
