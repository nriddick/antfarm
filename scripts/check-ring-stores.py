#!/usr/bin/env python3
"""Fail if AntFarm.writeImpl stores a ring word with a plain assignment.

Ring words are reused by later laps while a lagging consumer may still probe
them (the 7b mid-tick yield check), so every store in writeImpl goes through
storeRaw/storeRawRange or an atomicStore. A plain `w[i] = v` would be a formal
data race that TSan is unlikely to catch. The check runs a built-in negative
control first, so a broken extractor or pattern cannot pass silently.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
RING = r"(?:w|ph|buf)"
PLAIN_STORE = re.compile(
    r"(?:\b" + RING + r"\s*\[[^\]]*\]|\*\s*\(?\s*" + RING + r"\b[^=;]*?)\s*(?:[-+|&^]|<<|>>)?=(?!=)")


def write_impl_body(text):
    start = text.index("private ulong writeImpl(")
    open_brace = text.index("{", start)
    depth = 0
    for i in range(open_brace, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[open_brace:i + 1]
    raise ValueError("unterminated writeImpl body")


def plain_stores(body):
    hits = []
    for line in body.splitlines():
        code = line.split("//", 1)[0]
        if PLAIN_STORE.search(code):
            hits.append(line.strip())
    return hits


def main():
    text = (ROOT / "antfarm/package.d").read_text()
    body = write_impl_body(text)
    if body.count("storeRaw(") < 10:
        print("check-ring-stores: writeImpl extraction found too few storeRaw calls", file=sys.stderr)
        return 1
    controls = [
        ("storeRaw(w[1], wtprime);", "w[1] = wtprime;"),
        ("storeRaw(ph[0], cast(ulong) cast(void*) hp);", "ph[0] = cast(ulong) cast(void*) hp;"),
        ("storeRaw(w[1], wtprime);", "*(w + 1) = wtprime;"),
        ("storeRaw(w[1], wtprime);", "w[1] |= wtprime;"),
    ]
    for original, mutant in controls:
        if original not in body or not plain_stores(body.replace(original, mutant, 1)):
            print(f"check-ring-stores: negative control not detected: {mutant}", file=sys.stderr)
            return 1
    hits = plain_stores(body)
    for hit in hits:
        print(f"check-ring-stores: plain ring store in writeImpl: {hit}", file=sys.stderr)
    if hits:
        return 1
    print(f"PASS writeImpl ring stores are raw atomics ({len(controls)} negative controls detected)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
