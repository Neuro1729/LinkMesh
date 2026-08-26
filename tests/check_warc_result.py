#!/usr/bin/env python3
"""Asserts the exact backlink index produced from tests/make_test_warc.py.

This pins down URL normalization, which is the part that fails silently. A
pipeline with broken normalization still runs to completion and still emits a
plausible-looking index, so only an exact expected result catches it.
"""
import sys

ALPHA = "https://alpha.example/index.html"
BETA = "https://beta.example/home"
GAMMA = "https://gamma.example/x?q=1&r=2"

# alpha links out to beta and gamma. Beta appears four times in the source HTML
# (plain, with a fragment, uppercase host, explicit :443) and all four must
# collapse to one edge. mailto: and javascript: are dropped. /about is same-site
# so the default external-only filter removes it.
#
# beta uses <base href="https://beta.example/docs/">, so its two relative links
# resolve inside beta.example and are dropped as same-site. Only alpha survives.
EXPECTED = {
    ALPHA: {BETA, GAMMA},
    BETA: {ALPHA, GAMMA},
    GAMMA: {ALPHA},
}


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "output/backlinks.tsv"
    actual = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if not line:
                continue
            target, _, sources = line.partition("\t")
            actual[target] = set(sources.split())

    problems = []
    for target, sources in EXPECTED.items():
        if target not in actual:
            problems.append(f"missing key: {target}")
        elif actual[target] != sources:
            problems.append(
                f"{target}\n     expected {sorted(sources)}\n     got      {sorted(actual[target])}")

    for target in actual:
        if target not in EXPECTED:
            problems.append(f"unexpected key: {target} <- {sorted(actual[target])}")

    if problems:
        print("URL normalization check FAILED:")
        for problem in problems:
            print("  " + problem)
        return 1

    print(f"URL normalization OK: {len(actual)} keys exactly as expected")
    print("  fragment, default port, host case, and HTML entity all normalized")
    print("  mailto/javascript dropped, same-site links filtered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
