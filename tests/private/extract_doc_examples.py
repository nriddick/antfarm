#!/usr/bin/env python3
"""Extract the complete private-actor documentation programs for compilation.

The six complete programs are spread across the private-actor guides and the
sizing guide, next to the prose that explains them. Illustrative snippets are
marked as fragments and rejected if they look like a complete program without
a `void main`.
"""
from pathlib import Path
import re

root = Path(__file__).resolve().parent
sources = (
    root.parents[1] / "docs/guide/private-actors.md",
    root.parents[1] / "docs/guide/private-results-and-waits.md",
    root.parents[1] / "docs/guide/outcomes-and-cancellation.md",
    root.parents[1] / "docs/guide/sizing-and-allocation.md",
)
programs = []
for source in sources:
    for index, block in enumerate(re.findall(r"```d\n(.*?)\n```", source.read_text(), re.S)):
        if "void main" in block:
            programs.append(block)
        elif "fragment" not in block:
            raise SystemExit(
                f"{source.name} ```d block {index} is neither a complete program "
                "nor marked as a fragment")
if len(programs) != 6:
    raise SystemExit(
        "expected the root, conductor, child-fold, subset, root-tracking and "
        f"cohort-slab documentation programs, found {len(programs)}")
output = root / ".doc-examples"
output.mkdir(exist_ok=True)
for index, program in enumerate(programs):
    (output / f"private_doc_{index}.d").write_text(program + "\n")
