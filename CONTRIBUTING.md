# Contributing

## Change workflow

1. Open an issue with a problem statement and observable acceptance criteria.
2. Create a short-lived branch named `feat/<topic>`, `fix/<topic>`, or `docs/<topic>`.
3. Keep commits cohesive and explain why the change exists.
4. Add or update tests and operational documentation with the implementation.
5. Run `uv run ruff check .` and `uv run pytest` before requesting review.
6. Squash only noisy fixups; preserve meaningful engineering steps.

## Definition of done

A change is done when behavior, tests, documentation, and verification evidence agree.
Planned behavior must be labelled as planned. Never add credentials or generated data
volumes to the repository.

## Pull request template

- Problem and operational impact
- Scope and deliberate non-goals
- Design and trade-offs
- Verification evidence
- Rollback or recovery path
- Follow-up work

