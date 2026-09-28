"""Validate repository work-order and Conventional Commit policy.

The validator is deliberately read-only: all Git/GitHub context is supplied as CLI
arguments so CI can decide how to obtain branch, PR title, and introduced commit
subjects without this module mutating the repository.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence

WORK_ORDER_PATTERN = r"xdl-pr\d{3}-[a-z0-9]+(?:-[a-z0-9]+)*"
WORK_ORDER_RE = re.compile(rf"(?P<work_order>{WORK_ORDER_PATTERN})")
BRANCH_RE = re.compile(rf"^[a-z][a-z0-9-]*/(?P<work_order>{WORK_ORDER_PATTERN})$")
CONVENTIONAL_RE = re.compile(
    rf"^(?:feat|fix|refactor|test|docs|chore|ci|build)\((?P<work_order>{WORK_ORDER_PATTERN})\): .+"
    r"$"
)


class PolicyError(ValueError):
    """Raised when Git metadata violates the repository policy."""


def work_order_from_branch(branch: str) -> str:
    """Return the exact work-order encoded by a valid implementation branch."""
    match = BRANCH_RE.fullmatch(branch.strip())
    if match is None:
        raise PolicyError(f"invalid work-order branch: {branch!r}")
    return match.group("work_order")


def validate_commit_subject(subject: str, work_order: str) -> None:
    """Validate one Conventional Commit subject against the branch work-order."""
    match = CONVENTIONAL_RE.fullmatch(subject.strip())
    if match is None:
        raise PolicyError(f"invalid Conventional Commit subject: {subject!r}")
    if match.group("work_order") != work_order:
        raise PolicyError(
            f"commit work-order {match.group('work_order')!r} does not match {work_order!r}"
        )


def validate_pr_title(title: str, work_order: str) -> None:
    """Validate the PR title and require the same exact work-order scope."""
    validate_commit_subject(title, work_order)


def validate_policy(
    *, branch: str, commit_subjects: Sequence[str], pr_title: str | None = None
) -> None:
    """Validate branch, all introduced commits, and optional PR title."""
    review_match = REVIEW_BRANCH_RE.fullmatch(branch.strip())
    if review_match is not None:
        review_date = review_match.group("date")
        expected_commit = f"docs(review): weekly repository review {review_date}"
        expected_title = f"Weekly repository review -- {review_date}"
        if not commit_subjects:
            raise PolicyError("at least one introduced commit subject is required")
        for subject in commit_subjects:
            if subject.strip() != expected_commit:
                raise PolicyError(
                    f"invalid weekly review commit subject: {subject!r}; "
                    f"expected {expected_commit!r}"
                )
        if pr_title is not None and pr_title.strip() != expected_title:
            raise PolicyError(
                f"invalid weekly review PR title: {pr_title!r}; expected {expected_title!r}"
            )
        return

    work_order = work_order_from_branch(branch)
    if not commit_subjects:
        raise PolicyError("at least one introduced commit subject is required")
    for subject in commit_subjects:
        validate_commit_subject(subject, work_order)
    if pr_title is not None:
        validate_pr_title(pr_title, work_order)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--commit", action="append", dest="commits", default=[])
    parser.add_argument("--pr-title")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate_policy(branch=args.branch, commit_subjects=args.commits, pr_title=args.pr_title)
    except PolicyError as exc:
        print(f"git policy violation: {exc}")
        return 1
    print("git policy: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
