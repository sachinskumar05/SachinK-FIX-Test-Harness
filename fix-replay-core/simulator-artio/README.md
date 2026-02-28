# simulator-artio

Artio-based integration-test FIX simulator module. The simulator exposes two acceptor sessions:

- `ENTRY` session receives inbound order flow from the replayer.
- `EXIT` session sends routed/mutated flow back to the replayer.

## Run Locally

```bash
./gradlew :simulator-artio:run --args="--config simulator-artio/examples/simulator.yaml"
```

If rule-based mutation is enabled:

```bash
./gradlew :simulator-artio:run --args="--config simulator-artio/examples/simulator.yaml --rules simulator-artio/examples/rules.yaml"
```

## Run In CI

Use module checks in pipeline:

```bash
./gradlew :simulator-artio:test
./gradlew :simulator-artio:check
```

For end-to-end replay stages, start simulator with the same config file used in test fixtures:

```bash
./gradlew :simulator-artio:run --args="--config simulator-artio/examples/simulator.yaml"
```

## Replayer Connection Contract

The simulator hosts these acceptor endpoints by default:

- `ENTRY`:
  - Host/Port: `localhost:7001`
  - `BeginString=FIX.4.2`
  - Acceptor IDs: `SenderCompID=FIX_GATEWAY`, `TargetCompID=ENTRY_RACOMPID`
- `EXIT`:
  - Host/Port: `localhost:7002`
  - `BeginString=FIX.4.2`
  - Acceptor IDs: `SenderCompID=FIX_GATEWAY`, `TargetCompID=EXIT_RACOMPID`

The replayer should connect as initiator to both sessions with complementary IDs:

- Replayer -> `ENTRY` acceptor: `SenderCompID=ENTRY_RACOMPID`, `TargetCompID=FIX_GATEWAY`
- Replayer -> `EXIT` acceptor: `SenderCompID=EXIT_RACOMPID`, `TargetCompID=FIX_GATEWAY`

If ports or comp IDs are overridden in simulator config, the replayer must use matching values.
