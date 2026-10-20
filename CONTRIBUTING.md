# Contributing to steadypy

Thanks for your interest in improving steadypy. Bug reports, documentation
fixes, tests and code are all welcome.

## Before you start

- **Bugs.** Open an issue using the bug report form. A minimal script that
  reproduces the problem helps most. If timing is involved,
  `steadypy.testing.FakeClock` can often make the reproduction deterministic.
- **New features or API changes.** Please open an issue to discuss the idea
  before writing code. steadypy aims to stay small, so not every useful
  feature belongs in it.
- **Security issues.** Do not open a public issue; follow [SECURITY.md](SECURITY.md).

By participating you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## Development setup

You need Python 3.11 or newer.

```bash
git clone https://github.com/anika-kulkarni/steadypy
cd steadypy
python -m venv .venv
source .venv/bin/activate        # on Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Checks

CI runs all of these, and a pull request should pass them locally first:

```bash
pytest                             # the test suite
coverage run -m pytest && coverage report
ruff check .                       # lint
ruff format --check .              # formatting (run `ruff format .` to fix)
mypy src                           # type check (strict)
```

## Guidelines

**Every behavior change comes with a test.** A bug fix should start with a
test that fails without the fix.

**No real sleeps in tests.** Use `FakeClock` for anything time-based, and
`asyncio.Event`s or futures to control the order in which tasks finish. The
whole suite runs in about a second, and we want to keep it that way and keep
it free of flaky timing.

**Think about cancellation.** For any code that awaits, ask what happens when
the caller is cancelled at that point, and add a test for it.
`CancelledError` must always propagate, and every task steadypy starts must be
finished before control returns to the user. The test suite treats warnings
as errors, so a coroutine that is never awaited fails the build.

**No runtime dependencies.** steadypy depends only on the standard library.
Development tools are fine.

**Public API.** New public names need type hints, a docstring, an entry in
`steadypy.__all__`, and documentation in the README or `docs/`. Keep names
consistent with the existing API.

**Changelog.** Add a line to the *Unreleased* section of
[CHANGELOG.md](CHANGELOG.md) for anything a user would notice.

## Commits and pull requests

- Keep each pull request focused on one change.
- Commit messages follow the [Conventional Commits](https://www.conventionalcommits.org/)
  style used in the history (`feat:`, `fix:`, `docs:`, `test:`, `ci:`, `chore:`).
- Describe *why* the change is needed, not only what it does.
- Expect review comments; they are about the code, never about you.
