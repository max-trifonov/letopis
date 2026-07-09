# Security policy

## Supported versions

Letopis is pre-1.0. Only the `main` branch (latest commit) is supported
with security fixes; there are no maintained release branches yet. This
will be revisited once tagged releases start.

## Reporting a vulnerability

Please **do not** open a public issue for a security report.

Preferred: use [GitHub Security Advisories](https://github.com/max-trifonov/letopis/security/advisories/new)
for this repository — it opens a private channel with maintainers.

Fallback: email **mp.trifonov@gmail.com** with:

- a description of the issue and its impact,
- steps to reproduce (a minimal request/config is ideal),
- the version or commit you tested against.

We aim to acknowledge reports within 5 business days and to agree on a
disclosure timeline with the reporter once the issue is confirmed.

## Scope

In scope: the `letopis` binary and its packages in this repository —
ingest/read paths, authentication and tenant isolation, the rules and
webhook-delivery pipeline (including the SSRF guard in
`internal/delivery`), hash-chain integrity, and the plugin host.

Out of scope: vulnerabilities in third-party dependencies (report
upstream; we'll happily help route them), and issues that require an
already-compromised admin-scoped API key or root access to the host.

## Safe harbor

Good-faith research against your own Letopis instance — not production
data belonging to others — that avoids privacy violations, data
destruction, and service disruption is welcome and won't be treated as a
violation of this project's terms.
