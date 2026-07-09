# Contributing to Letopis

Thanks for your interest! A few ground rules:

- **DCO**: every commit must be signed off (`git commit -s`), certifying
  the [Developer Certificate of Origin](https://developercertificate.org/).
- **Before pushing**: `make test lint` must pass. Proto changes require
  `make proto` and committing the regenerated code.
- Significant changes deserve an issue first — the architecture follows
  recorded design decisions, and we'd rather discuss before you spend
  time on code.

Project layout: `cmd/letopis` is a thin entrypoint; the core is a
library wired in `internal/app`; the only public Go API is `pkg/ext`
(extension points, semver-stable).

By participating in this project you agree to abide by the
[Code of Conduct](CODE_OF_CONDUCT.md). Found a security issue? See
[SECURITY.md](SECURITY.md) instead of opening a public issue.
