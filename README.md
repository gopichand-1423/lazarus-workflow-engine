# Lazarus

Lazarus is a Java 24 durable workflow engine built with Spring Boot 4.1, virtual threads, PostgreSQL event sourcing, and gRPC. It includes ordered workflow replay, lease-based task claims with monotonic fencing tokens, deterministic activity idempotency keys, a payment saga with reverse compensation, and a seeded virtual-clock simulation harness.

## Runtime architecture

- **Workflow execution:** `LazarusEngine` executes local durable workflows. `WorkflowContext` records each step invocation by ordinal and replays completed output in event order. A changed step at a recorded ordinal raises `NonDeterministicWorkflowException`.
- **Side effects:** Activity execution is at least once. Each invocation receives a stable idempotency key derived from workflow ID, step ordinal, and step key. External adapters must honor that key to deduplicate retries.
- **Event store:** PostgreSQL stores workflow history in `workflow_events`. Versioned migrations install a PostgreSQL trigger that rejects event updates and deletes. H2 is supported for local development and tests; the PostgreSQL-only trigger is intentionally not installed there.
- **Task queue:** PostgreSQL claims use `FOR UPDATE SKIP LOCKED`; H2 uses its `FOR UPDATE` path. The selected application mode is checked against the connected database so a failed PostgreSQL connection cannot silently become H2.
- **Leases:** Every claim/reclaim increments a monotonic fencing token. Completion and heartbeat operations validate the current worker, token, task identity, and unexpired lease.
- **Services:** Spring Boot owns the gRPC server, virtual-thread execution, and zombie reaper lifecycles. PostgreSQL is the default runtime database.
- **Payment saga:** The local `PaymentSagaWorkflow` charges, reserves inventory, and ships. On failure it records compensations in reverse order: release inventory, then refund payment. The mock adapters deduplicate operations by idempotency key.
- **Simulation:** `DeterministicChaosClock` advances virtual time directly and uses a fixed seed for repeatable fault selection. The simulation runner covers lease recovery, crash replay, and saga compensation without sleeping.

## Requirements

- Java 24. The repository's `activate.ps1` selects the local JDK 24.0.2 toolchain.
- Maven 3.9.9, available through `activate.ps1` on Windows.
- Docker Compose for the PostgreSQL profile.

## Run locally on Windows

Activate the project toolchain, start PostgreSQL, and run the tests:

```powershell
. .\activate.ps1
docker compose up -d postgres
mvn test
```

The test profile uses H2 and runs the same common versioned migrations. Start the Spring Boot application against PostgreSQL with:

```powershell
mvn spring-boot:run
```

To run the application explicitly with local H2 instead, select the H2 profile:

```powershell
$env:SPRING_PROFILES_ACTIVE = "h2"
mvn spring-boot:run
Remove-Item Env:SPRING_PROFILES_ACTIVE
```

The Spring application exposes its gRPC server on port `9090` by default. Configure it with `LAZARUS_GRPC_PORT`; the zombie reaper can be configured with `LAZARUS_REAPER_ENABLED`, `LAZARUS_REAPER_INTERVAL_MS`, and `LAZARUS_REAPER_BATCH_SIZE`.

## Dashboard

The standalone dashboard binds to `127.0.0.1:8080` by default. It uses PostgreSQL by default and fails with a clear connection error instead of switching databases. To select H2 explicitly:

```powershell
$env:LAZARUS_DB_MODE = "h2"
mvn exec:java -Dexec.mainClass="dev.lazarus.ui.LazarusDashboardServer"
Remove-Item Env:LAZARUS_DB_MODE
```

To use PostgreSQL, start the Compose service and leave `LAZARUS_DB_MODE` unset or set it to `postgresql`. The dashboard can be configured with `LAZARUS_DB_URL`, `LAZARUS_DB_USER`, `LAZARUS_DB_PASS`, `LAZARUS_BIND_ADDRESS`, and `LAZARUS_PORT`. Mutating dashboard API routes accept POST only; database values are inserted into the page through DOM `textContent`.

## Deterministic simulation

The simulation defaults explicitly to an isolated H2 database and seed `42024`:

```powershell
mvn exec:java -Dexec.mainClass="dev.lazarus.dst.LazarusSimulationRunner" -Dexec.args="42024"
```

Set `LAZARUS_DB_MODE=postgresql` and the database connection variables to run it against PostgreSQL. The runner never changes database modes after a connection failure.

## Database migrations

Flyway applies versioned SQL from `src/main/resources/db/migration/common` and any matching database-specific migrations. V1 creates/upgrades the workflow, event, and task tables; V2 backfills legacy step identity. H2 uses the common migrations; PostgreSQL also applies V3, which protects the append-only event log. `docker compose up -d postgres` starts the database; the application applies migrations when it connects.

## Project layout

```text
src/main/java/dev/lazarus/
  config/       Spring configuration and database-mode validation
  dst/          Deterministic clock and simulation runner
  engine/       Durable workflow context, engine, registry, and reaper
  grpc/         gRPC service and server lifecycle
  store/        Event store, task queue, and migration helpers
  ui/           Standalone local dashboard
  workflow/     Payment saga and idempotency-aware mock services
src/main/proto/                 gRPC contract
src/main/resources/db/migration Versioned SQL migrations
```

## Guarantees and limits

Replay is sequence-aware and rejects workflow history divergence at an existing step ordinal. Lease fencing prevents stale workers from committing after a task is reclaimed. Activity execution can repeat after a crash; deterministic idempotency keys let cooperating downstream services deduplicate those retries. The engine does not claim exactly-once external side effects.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
