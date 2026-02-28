# fix-replay-core

Reusable FIX replay engine designed for:
- CLI execution in GitLab pipelines
- REST-backed web UI execution

## Modules

- `:fix-model` - FIX message model, tags, and parsing primitives
- `:fix-loader` - fast log scanning and extraction helpers
- `:fix-linker` - deterministic linking and tag discovery logic
- `:fix-compare` - configurable FIX message comparison and diffing
- `:fix-runner` - offline replay runner and online adapter interfaces
- `:adapter-artio` - optional Artio-flavored online adapter implementation
- `:simulator-artio` - Artio-based integration-test simulator with ENTRY/EXIT acceptor sessions
- `:adapter-quickfixj` - optional QuickFIX/J-flavored online adapter implementation
- `:app-cli` - picocli command-line entry point
- `:app-server` - small Javalin REST backend
- `:app-ui` - NodeJS UI placeholder folder

## Build And Test

```bash
./gradlew test
```

## CLI

```bash
./gradlew :app-cli:run --args="--help"
./gradlew :app-cli:run
```

Default run prints the current project version.

## Server

```bash
./gradlew :app-server:run
```

Default endpoints:
- `GET /health`
- `GET /version`
- `POST /replay/dry-run`

## CI

GitLab CI validates compilation and tests via `.gitlab-ci.yml`.

## Simulator (Artio)

See [simulator-artio/README.md](simulator-artio/README.md) for the simulator runbook.
