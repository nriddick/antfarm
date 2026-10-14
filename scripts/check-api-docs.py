#!/usr/bin/env python3
"""Check the API index and Ddoc coverage of the public runtime modules.

Runs DMD once in a scratch directory (no repository files are written):
`dmd -c -o- -D -Dd<scratch> -X -Xf=<scratch>/api.json` builds the
declaration inventory of every public module, including the version-gated
mimalloc adapter. `-D` is what makes DMD retain each declaration's Ddoc
comment in the JSON: without it the `comment` field is absent.

Two checks follow:

 1. Every symbol named in the Symbol column of the table in
    docs/reference/README.md (the API index; not every backticked token
    under docs/reference/) resolves to a public declaration in the
    inventory.
 2. Every public declaration in the documented modules (public top-level
    declarations and public members of public types) carries a non-empty
    Ddoc comment: its `comment` field, stripped of whitespace, must be
    non-empty. DMD reports `/// ditto` as the literal text "ditto", which
    counts as documented; a bare `///` yields only a newline and counts
    as undocumented. Coverage is per declaration: every overload counts
    on its own.

No network access. Requires DMD on PATH.
"""
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Modules whose public surface must carry Ddoc comments.
COVERAGE_MODULES = [
    "antfarm/package.d",
    "antfarm/templates.d",
    "antfarm/actors/actor.d",
    "antfarm/actors/wave.d",
    "antfarm/actors/private_actor.d",
    "farmhost/source/farmhost/package.d",
]

# Everything compiled for the inventory; the index may name symbols here.
INDEX_MODULES = COVERAGE_MODULES + [
    "antfarm/actors/package.d",
    "antfarm/actors/mimalloc.d",
]

# Kinds that carry a user-visible name and must be documented.
DOCUMENTED_KINDS = {
    "variable", "enum", "enum member", "alias", "struct", "class",
    "interface", "union", "function", "template", "constructor",
    "copy constructor",
}
SKIPPED_KINDS = {"import", "unittest", "generated function"}
TYPE_KINDS = {"struct", "class", "interface", "union", "enum"}

INDEX_PAGE = ROOT / "docs/reference/README.md"


def compile_inventory(scratch):
    if shutil.which("dmd") is None:
        raise SystemExit(
            "check-api-docs: dmd not found on PATH; "
            "install DMD to run the API doc check")
    arguments = ["-c", "-o-", "-D", "-Dd.", "-X", "-Xf=api.json",
                 "-version=AntfarmMimallocV3", "-I" + str(ROOT),
                 "-I" + str(ROOT / "threadpool/source"),
                 "-I" + str(ROOT / "farmhost/source")]
    arguments += [str(ROOT / m) for m in INDEX_MODULES]
    try:
        result = subprocess.run(["dmd"] + arguments, cwd=scratch,
                                capture_output=True, text=True)
    except FileNotFoundError:
        raise SystemExit(
            "check-api-docs: dmd not found on PATH; "
            "install DMD to run the API doc check")
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise SystemExit(
            f"check-api-docs: dmd failed: {' '.join(arguments)}")
    return json.load(open(os.path.join(scratch, "api.json")))


def strip_comments(line, state):
    """One source line with //, /* */ and nested /+ +/ comments removed.

    `state` is the mutable stack of open block-comment closers, carried
    across lines ("/+ ... +/" nests; "/* ... */" does not).
    """
    out = []
    index = 0
    while index < len(line):
        if state:
            closer = state[-1]
            close = line.find(closer, index)
            if closer == "+/":
                open_again = line.find("/+", index)
                if open_again != -1 and (close == -1 or open_again < close):
                    state.append("+/")
                    index = open_again + 2
                    continue
            if close != -1:
                state.pop()
                index = close + 2
                continue
            index = len(line)
            continue
        markers = {line.find(marker, index): marker
                   for marker in ("//", "/*", "/+") if line.find(marker, index) >= 0}
        if not markers:
            out.append(line[index:])
            break
        first = min(markers)
        out.append(line[index:first])
        if markers[first] == "//":
            break  # the rest of the line is a line comment
        state.append("*/" if markers[first] == "/*" else "+/")
        index = first + 2
    return "".join(out)


def scan_protections(path):
    """Best-effort map of source line number to protection keyword.

    `dmd -X` omits the protection of members nested inside templated types,
    so the checker falls back to scanning the declaration's own source line:
    protection labels (`private:`) apply to the current brace depth until the
    next label or a closing brace, and a `private`/`package` prefix applies
    to the single declaration that follows it on the same line.
    """
    protections = {}
    stripped = []
    state = []  # closers of block comments open across lines
    for line in path.read_text(errors="replace").splitlines():
        stripped.append(strip_comments(line, state))
    labels = ["public"]
    for number, line in enumerate(stripped, 1):
        label = re.match(r"\s*(public|private|package|protected|export)\s*:",
                         line)
        if label:
            labels[-1] = label.group(1)
            continue
        # A nested block (function body, static if, version) inherits the
        # active protection; only a label or a declaration prefix changes it.
        opening = line.count("{")
        closing = line.count("}")
        prefix = re.match(r"\s*(public|private|package|protected|export)\b",
                          line)
        protections[number] = prefix.group(1) if prefix else labels[-1]
        labels.extend([labels[-1]] * opening)
        for _ in range(closing):
            if len(labels) > 1:
                labels.pop()
    return protections


def is_public(member, protections=None):
    if "protection" in member:
        return member["protection"] == "public"
    if protections is not None and "line" in member:
        return protections.get(member["line"], "public") == "public"
    return True


def is_required(member, protections=None):
    name = member.get("name", "")
    # A type whose fields have destructors gets a compiler-generated `~this`
    # alias with no source line; there is nothing to document.
    generated_dtor = name == "~this" and "line" not in member
    return (is_public(member, protections)
            and member.get("kind") in DOCUMENTED_KINDS
            and member.get("kind") not in SKIPPED_KINDS
            and not name.startswith("__")
            and not generated_dtor)


def is_documented(member):
    return bool(member.get("comment", "").strip())


def public_declarations(members, protections):
    """Yield (qualified name, declaration) for a module's public surface."""
    for member in members:
        if not is_required(member, protections):
            continue
        yield member["name"], member
        body = None
        if member.get("kind") in TYPE_KINDS:
            body = member
        elif member.get("kind") == "template":
            # A templated type nests a same-named body inside the template.
            body = next((nested for nested in member.get("members", [])
                         if nested.get("name") == member["name"]
                         and nested.get("kind") in TYPE_KINDS), None)
        if body is not None:
            for nested in body.get("members", []):
                if is_required(nested, protections):
                    yield f"{member['name']}.{nested['name']}", nested


def resolution_names(api):
    """Every public name the index may reference: module names, top-level
    declarations, and Type.member paths."""
    names = set()
    for module in api:
        names.add(module["name"])
        protections = scan_protections(ROOT / module["file"]) \
            if module.get("file") else None
        for qualified, _ in public_declarations(
                module.get("members", []), protections):
            names.add(qualified)
    return names


def index_symbols():
    """Backticked tokens in the Symbol column of the API index table."""
    symbols = []
    for line in INDEX_PAGE.read_text().splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 2 or cells[1] in {"Symbol", "---", ":---", ":-:"}:
            continue
        symbols.extend(re.findall(r"`([^`]+)`", cells[1]))
    return symbols


def main():
    with tempfile.TemporaryDirectory() as scratch:
        api = compile_inventory(scratch)

    errors = []
    resolvable = resolution_names(api)
    for symbol in index_symbols():
        if symbol not in resolvable:
            errors.append(f"index symbol not found: {symbol}")

    coverage = {}
    for module in api:
        # Full paths: several modules are package.d files.
        if os.path.normpath(ROOT / module.get("file", "")) not in {
                os.path.normpath(ROOT / m) for m in COVERAGE_MODULES}:
            continue
        protections = scan_protections(ROOT / module["file"])
        required = 0
        missing = []
        for qualified, member in public_declarations(
                module.get("members", []), protections):
            required += 1
            if not is_documented(member):
                missing.append(f"{qualified} (line {member.get('line')})")
        coverage[module["name"]] = (required - len(missing), required)
        for name in missing:
            errors.append(f"{module['name']}: undocumented: {name}")

    documented = sum(done for done, _ in coverage.values())
    required = sum(total for _, total in coverage.values())
    for name, (done, total) in sorted(coverage.items()):
        print(f"api docs: {name}: {done}/{total} documented")
    print(f"api docs: {documented}/{required} public declarations documented "
          f"({100 * documented // required}%) across {len(coverage)} modules")
    for error in errors:
        print(f"api docs: {error}")
    if errors:
        return 1
    print("api docs: index symbols resolve and coverage is complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
