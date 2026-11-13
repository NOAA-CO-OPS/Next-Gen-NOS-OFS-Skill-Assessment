#!/usr/bin/env python3
"""Summarize coverage.xml and enforce the repo coverage floor (no Coveralls).

Examples
--------
Print a short summary::

    python scripts/coverage_summary.py coverage.xml

Enforce the floor from pyproject.toml::

    python scripts/coverage_summary.py coverage.xml --fail-under-from-pyproject

Reject a PR that lowers ``[tool.coverage.report] fail_under`` vs the base branch::

    python scripts/coverage_summary.py --check-floor-not-lowered --base-ref origin/main
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def parse_coverage_xml(path: Path) -> tuple[int, int, float]:
    """Return (statements, missing, percent) from a Cobertura coverage.xml."""
    root = ET.parse(path).getroot()
    line_rate = float(root.attrib.get('line-rate', '0'))
    # Prefer sums from packages/classes when present; fall back to rate only.
    statements = 0
    covered = 0
    for cls in root.findall('.//class'):
        lines = cls.find('lines')
        if lines is None:
            continue
        for line in lines.findall('line'):
            statements += 1
            if int(line.attrib.get('hits', '0')) > 0:
                covered += 1
    if statements == 0:
        # Some generators only set aggregate rates on the root element.
        percent = round(line_rate * 100.0, 2)
        return 0, 0, percent
    missing = statements - covered
    percent = round(100.0 * covered / statements, 2)
    return statements, missing, percent


def _fail_under_from_toml_text(text: str) -> int | None:
    """Parse fail_under via tomllib; return None if missing/invalid."""
    try:
        data = tomllib.loads(text)
        value = data['tool']['coverage']['report']['fail_under']
        return int(value)
    except (tomllib.TOMLDecodeError, KeyError, TypeError, ValueError):
        return None


def _fail_under_from_regex(text: str) -> int | None:
    """Regex fallback for incomplete or oddly formatted TOML text."""
    match = re.search(
        r'\[tool\.coverage\.report\](.*?)(?:\n\[|\Z)',
        text,
        flags=re.DOTALL,
    )
    section = match.group(1) if match else text
    found = re.search(r'(?m)^fail_under\s*=\s*(\d+)\s*$', section)
    if not found:
        return None
    return int(found.group(1))


def read_fail_under_from_pyproject(pyproject: Path) -> int:
    """Parse fail_under from [tool.coverage.report] using tomllib."""
    text = pyproject.read_text(encoding='utf-8')
    value = _fail_under_from_toml_text(text)
    if value is None:
        raise SystemExit(
            f'Could not find fail_under in [tool.coverage.report] ({pyproject})'
        )
    return value


def read_fail_under_from_ref(
    base_ref: str, pyproject_rel: str = 'pyproject.toml'
) -> int | None:
    """Read fail_under from pyproject.toml at a git ref.

    Prefers tomllib on ``git show`` stdout; falls back to regex if parsing
    fails. Returns None if the file or setting is missing on that ref (first
    introduction of the floor is allowed).
    """
    try:
        proc = subprocess.run(
            ['git', 'show', f'{base_ref}:{pyproject_rel}'],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return None
    return _fail_under_from_toml_text(proc.stdout) or _fail_under_from_regex(
        proc.stdout
    )


def print_summary(path: Path, statements: int, missing: int, percent: float) -> None:
    covered = max(statements - missing, 0)
    print('Coverage summary (repo-only; no Coveralls)')
    print(f'  file:        {path}')
    if statements:
        print(f'  statements:  {statements}')
        print(f'  covered:     {covered}')
        print(f'  missing:     {missing}')
    print(f'  TOTAL:       {percent:.2f}%')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'coverage_xml',
        nargs='?',
        type=Path,
        help='Path to coverage.xml (Cobertura)',
    )
    parser.add_argument(
        '--fail-under',
        type=int,
        default=None,
        help='Minimum TOTAL percent allowed (integer)',
    )
    parser.add_argument(
        '--fail-under-from-pyproject',
        action='store_true',
        help='Read fail_under from pyproject.toml [tool.coverage.report]',
    )
    parser.add_argument(
        '--pyproject',
        type=Path,
        default=None,
        help='Path to pyproject.toml (default: repo root)',
    )
    parser.add_argument(
        '--print-fail-under',
        action='store_true',
        help='Print fail_under from pyproject.toml and exit',
    )
    parser.add_argument(
        '--check-floor-not-lowered',
        action='store_true',
        help='Fail if this branch lowers fail_under vs --base-ref',
    )
    parser.add_argument(
        '--base-ref',
        default='origin/main',
        help='Git ref for floor comparison (default: origin/main)',
    )
    args = parser.parse_args(argv)

    root = _repo_root()
    pyproject = args.pyproject or (root / 'pyproject.toml')

    if args.print_fail_under:
        print(read_fail_under_from_pyproject(pyproject))
        return 0

    if args.check_floor_not_lowered:
        head_floor = read_fail_under_from_pyproject(pyproject)
        base_floor = read_fail_under_from_ref(args.base_ref)
        if base_floor is None:
            print(
                f'Coverage floor: head={head_floor}  {args.base_ref}=<unset> '
                '(first introduction OK)'
            )
        else:
            print(
                f'Coverage floor: head={head_floor}  {args.base_ref}={base_floor}'
            )
            if head_floor < base_floor:
                print(
                    f'ERROR: fail_under lowered from {base_floor} to {head_floor}. '
                    'Ratchet up only; lowering needs maintainer approval.',
                    file=sys.stderr,
                )
                return 1
        print('Coverage floor OK (not lowered vs base).')
        if args.coverage_xml is None:
            return 0

    if args.coverage_xml is None:
        parser.error('coverage_xml is required unless only --check-floor-not-lowered')

    path = args.coverage_xml
    if not path.is_file():
        print(f'ERROR: coverage file not found: {path}', file=sys.stderr)
        return 1

    statements, missing, percent = parse_coverage_xml(path)
    print_summary(path, statements, missing, percent)

    fail_under = args.fail_under
    if args.fail_under_from_pyproject:
        fail_under = read_fail_under_from_pyproject(pyproject)
        print(f'  fail_under:  {fail_under} (from pyproject.toml)')

    if fail_under is not None:
        # Match coverage.py style: compare rounded percent to the integer floor.
        rounded = round(percent)
        if rounded < fail_under:
            print(
                f'ERROR: coverage {percent:.2f}% (rounded {rounded}%) '
                f'is below fail_under={fail_under}',
                file=sys.stderr,
            )
            return 1
        print(f'Coverage gate passed (>= {fail_under}%).')

    return 0


if __name__ == '__main__':
    sys.exit(main())
