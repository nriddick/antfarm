#!/usr/bin/env python3
"""Extract the complete private-actor documentation programs for compilation."""
from pathlib import Path
import re

root = Path(__file__).resolve().parent
programs = re.findall(r"```d\n(.*?)\n```", (root.parent / "actors/PRIVATE_ACTORS.md").read_text(), re.S)
if len(programs) != 4:
    raise SystemExit("expected the root, conductor, child-fold and subset documentation programs")
output = root / ".doc-examples"
output.mkdir(exist_ok=True)
for index, program in enumerate(programs):
    (output / f"private_doc_{index}.d").write_text(program + "\n")
