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

GitLab CI validates compilation/tests and runs the sample online scenario via `.gitlab-ci.yml`.

Pipeline artifacts are uploaded with `when: always` so they remain browsable whether jobs pass or fail:
- Gradle test reports and test-results XML
- `results/**` (online/offline JSON + JUnit + console log, named as `<scenario>-<yyyyMMdd-HHmmssSSS>-...`)
- `test-suites/**/.simulator-artio/logs/**`

## Simulator (Artio)

See [simulator-artio/README.md](simulator-artio/README.md) for the simulator runbook.
