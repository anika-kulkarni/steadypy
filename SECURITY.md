# Security policy

## Supported versions

steadypy has not reached 1.0. Security fixes are made for the latest release
line only, which is currently 0.1.x.

## Reporting a vulnerability

Please **do not open a public issue** for a security problem.

Report it privately through GitHub instead: open the repository's
**Security** tab and choose **Report a vulnerability**. GitHub's
[documentation](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
explains the process. Please include:

- the steadypy and Python versions affected,
- a description of the problem and its impact,
- a minimal way to reproduce it.

steadypy is maintained by volunteers, so reports are handled on a
best-effort basis. You will be kept informed, and credited in the advisory
unless you prefer not to be.

## Scope

steadypy has no runtime dependencies and does no network or file I/O of its
own. The issues most likely to matter are ones that break its guarantees in
a way that could hurt a service: a task that outlives its supervisor, a rate
limiter that admits more than it should, a circuit breaker that never
recovers, or signal handling that leaves a process unable to shut down.
