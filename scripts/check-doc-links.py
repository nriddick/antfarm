#!/usr/bin/env python3
"""Check tracked Markdown relative links and heading anchors.

Only tracked Markdown is scanned, external `scheme:` links are never fetched,
and no network access is used. A local target must be lexically inside the
repository, must not escape it through a symlink, and must itself be tracked.
Exit status 0 means clean; 1 means at least one broken, escaping, or
non-relative local link.
"""
import collections
import os
import pathlib
import re
import subprocess
import sys
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parent.parent
ROOT_PATH = os.path.normpath(str(ROOT))
REAL_ROOT = os.path.realpath(ROOT)
SKIP_PREFIXES = ("third_party/",)


def is_within(root, path):
    root = os.path.normpath(root)
    path = os.path.normpath(path)
    return path == root or path.startswith(root + os.sep)


def repo_path(name):
    """Repo-relative name to a normalized absolute path; no symlink resolution."""
    return os.path.normpath(os.path.join(ROOT_PATH, name))


def git_ls_files(*patterns):
    return subprocess.check_output(
        ["git", "ls-files", "-z", *patterns], cwd=ROOT, text=True).split("\0")


def tracked_markdown():
    found = []
    for name in git_ls_files("*.md"):
        if not name or name.startswith(SKIP_PREFIXES):
            continue
        path = pathlib.Path(repo_path(name))
        if os.path.islink(path) and not is_within(REAL_ROOT, os.path.realpath(path)):
            continue  # a tracked .md symlink that leaves the repository
        found.append(path)
    return found


def tracked_sets():
    """Lexically normalized tracked files and the tracked directories above them."""
    files = set()
    dirs = set()
    for name in git_ls_files():
        if not name:
            continue
        path = repo_path(name)
        files.add(path)
        parent = os.path.dirname(path)
        while parent != ROOT_PATH and is_within(ROOT_PATH, parent):
            dirs.add(parent)
            parent = os.path.dirname(parent)
    dirs.add(ROOT_PATH)
    return files, dirs


def unfence(text):
    """Blank out fenced code so links inside examples are ignored."""
    active = None  # (char, length) of the opening fence
    out = []
    for line in text.splitlines():
        match = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if match:
            char = match.group(1)[0]
            length = len(match.group(1))
            rest = match.group(2)
            if active is None:
                active = (char, length)
            elif char == active[0] and length >= active[1] and rest.strip() == "":
                active = None
            out.append("")
            continue
        out.append("" if active else line)
    return out


def strip_inline_code(line):
    """Blank out CommonMark code spans: a run of N backticks closes on a run of N.

    A backtick run preceded by an odd number of backslashes is escaped and is
    not a delimiter.
    """
    runs = []
    for match in re.finditer(r"`+", line):
        backslashes = 0
        position = match.start() - 1
        while position >= 0 and line[position] == "\\":
            backslashes += 1
            position -= 1
        if backslashes % 2 == 0:
            runs.append(match)
    out = list(line)
    index = 0
    while index < len(runs):
        opener = runs[index]
        length = len(opener.group())
        closer_index = None
        for candidate in range(index + 1, len(runs)):
            if len(runs[candidate].group()) == length:
                closer_index = candidate
                break
        if closer_index is None:
            index += 1
            continue
        for position in range(opener.start(), runs[closer_index].end()):
            out[position] = " "
        index = closer_index + 1
    return "".join(out)


def anchor_slug(text):
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)  # inline links/images
    text = re.sub(r"!?\[([^\]]*)\]\[[^\]]*\]", r"\1", text)  # reference links
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[`*_]", "", text).lower()
    text = "".join(c for c in text if c.isalnum() or c in "_- ")
    return text.replace(" ", "-")


def is_setext_underline(line):
    return re.fullmatch(r"=+|-+", line.strip()) is not None


def is_paragraph_line(line):
    """A line that could be the subject of a setext underline."""
    stripped = line.strip()
    if not stripped:
        return False
    if re.match(r"^#{1,6}(\s|$)", stripped):
        return False
    if re.match(r"^(>|\||```|~~~)", stripped):
        return False
    if re.match(r"^([-*+]|\d+[.)])\s", stripped):
        return False
    if line.startswith("    "):
        return False
    return True


def anchors(path):
    hits = collections.Counter()
    ids = set()

    def add(text):
        slug = anchor_slug(text)
        if not slug:
            return
        count = hits[slug]
        hits[slug] += 1
        ids.add(slug + ("-" + str(count) if count else ""))

    previous = None
    for line in unfence(path.read_text(errors="replace")):
        if is_setext_underline(line):
            if previous is not None:
                add(previous)
            previous = None
            continue
        if not line.strip():
            previous = None
            continue
        heading = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*$", line)
        if heading:
            add(heading[1])
            previous = None
        elif is_paragraph_line(line):
            previous = line.strip()
        else:
            previous = None
        ids.update(re.findall(r'(?:id|name)=["\']([^"\']+)', line))
    return ids


def main():
    files = tracked_markdown()
    tracked, tracked_dirs = tracked_sets()
    anchor_cache = {}
    broken = []
    absolute = []
    total = 0
    for path in files:
        relative = os.path.relpath(str(path), ROOT_PATH)
        for number, line in enumerate(unfence(path.read_text(errors="replace")), 1):
            masked = strip_inline_code(line)
            targets = re.findall(
                r'!?\[.*?\]\(\s*(<[^>]+>|[^\s)]+)(?:\s+["\'][^\n]*?)?\s*\)', masked)
            definition = re.match(r"^\s*\[.*?\]:\s*(\S+)", masked)
            if definition:
                targets.append(definition[1])
            for target in targets:
                target = target.strip("<>")
                total += 1
                if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", target):
                    continue  # external scheme; never fetched
                part, _, frag = target.partition("#")
                part = urllib.parse.unquote(part).partition("?")[0]
                if part:
                    joined = os.path.normpath(os.path.join(str(path.parent), part))
                    if not is_within(ROOT_PATH, joined):
                        if part.startswith("/"):
                            absolute.append([relative, number, target])
                        else:
                            broken.append([relative, number, target,
                                           "link escapes the checkout"])
                        continue
                    if os.path.exists(joined) or os.path.islink(joined):
                        if not is_within(REAL_ROOT, os.path.realpath(joined)):
                            broken.append([relative, number, target,
                                           "escapes the checkout through a symlink"])
                            continue
                    if joined not in tracked and joined not in tracked_dirs:
                        broken.append([relative, number, target,
                                       "path is missing or not tracked"])
                        continue
                else:
                    joined = str(path)
                if frag and joined.endswith(".md"):
                    if joined not in anchor_cache:
                        anchor_cache[joined] = anchors(pathlib.Path(joined))
                    if urllib.parse.unquote(frag) not in anchor_cache[joined]:
                        broken.append([relative, number, target, "missing heading anchor"])

    for entry in absolute:
        print(f"non-relative link: {entry[0]}:{entry[1]}: {entry[2]}")
    for entry in broken:
        print(f"broken link: {entry[0]}:{entry[1]}: {entry[2]} ({entry[3]})")
    print(f"check-doc-links: {len(files)} files, {total} links, "
          f"{len(broken)} broken, {len(absolute)} non-relative")
    return 1 if broken or absolute else 0


if __name__ == "__main__":
    sys.exit(main())
