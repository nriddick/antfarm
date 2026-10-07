#!/usr/bin/env python3
"""Extract public benchmark hot calls from saved LDC assembly."""
from pathlib import Path
import re
import sys
for name in sys.argv[1:]:
    p = Path(name)
    current = ''
    rows = []
    for line in p.read_text().splitlines():
        match = re.match(r'\s*\.type\s+([^,]+),@function', line)
        if match:
            current = match[1]
        if re.search(r'\b(callq?|jmpq?)\s.*(requestRetire|requestActorRetire|reclaimActor|reclaim|wake|markActorRetiring|driveActorRetirement|returnFreeSlot|closeSubmissionGate)', line):
            rows.append(current + '\n    ' + line.strip())
    p.with_name(p.stem + '-callers.txt').write_text('\n'.join(rows) + '\n')
    print(p.stem + '\n' + '\n'.join(rows))
