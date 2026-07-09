---
name: Bug report
about: Something in Letopis doesn't behave as documented
title: ""
labels: bug
---

**Describe the bug**
A clear description of what's wrong and what you expected instead.

**Reproduce**
Minimal steps to trigger it — config, request(s), collection state.
A `curl` sequence or a failing test is ideal.

**Environment**
- Letopis version/commit: `letopis --version` or the git SHA
- Role: `api` / `worker` / `all`
- MongoDB / Redis versions
- Deployment: binary / Docker / docker-compose

**Logs**
Relevant log lines (`log.format: json` makes these easy to paste). Redact
tenant IDs, API keys, and entity data you don't want public.
