## [0.3.0] - 2026-10-19

### 🚀 Features

- Prefix consumer names with the hostname
- Sweep empty stale consumers on reclaim
- Add on_dead notify hook to TaskSpec and worker
- Stamp enqueued_at with full-precision seconds
- Promote the worker tuning knobs to BinomicConfig
- *(otel)* Add the optional OpenTelemetry gateway module
- *(broker)* Persist delivery trace context in a side hash
- *(client)* Emit producer spans behind the telemetry switch
- *(worker)* Emit process spans, metrics and lifecycle logs

### 🐛 Bug Fixes

- *(client)* Only hand trace context to capable brokers
- *(telemetry)* Harden degradation paths and log retry scheduling
- *(master)* Include the master pid in consumer names

### 📚 Documentation

- Update changelog
- Document the on_dead dead-letter hook
- Tick the multi-instance roadmap item
- Describe the consumer sweep in both reliability guides
- Mark bounded storage done and correct its premise
- Tick the configuration roadmap item
- Cover the new BinomicConfig knobs in both quick starts
- Add the observability guide in both languages
- Document the custom broker telemetry fallback
- Align observability docs with the implementation
- Update the consumer naming contract

### 🎨 Styling

- Wrap long reclaim call sites

### 🧪 Testing

- Cover on_dead hook paths and steady the timeout tests
- Cover the consumer sweep on real Redis
- *(integration)* Verify telemetry collection inside a worker subprocess
- *(integration)* Wait for consumer state instead of reading once

### ⚙️ Miscellaneous Tasks

- Upgrade to v0.3.0
## [0.2.0] - 2026-10-18

### 🚀 Features

- Client lifecycle control by contextmanager
- Structlog formate

### 🐛 Bug Fixes

- Complete the client lifecycle seams

### 🚜 Refactor

- Msg schema
- Rework the broker, worker and client pipeline

### 📚 Documentation

- Point Message references at the broker module
- Align guides and README with the reworked pipeline

### 🎨 Styling

- Apply ruff formatting to test files

### 🧪 Testing

- Move message schema tests into the broker suite
- Expect sorted keys in BaseStruct JSON output
- Rewrite client tests for the async context lifecycle
- Align unit and integration suites with the reworked pipeline
- Create the sample app in client factory tests

### ⚙️ Miscellaneous Tasks

- Pin pre-commit ruff to v0.17.0
- Add missing trailing newlines
- Update tooling configuration
- Upgrade to v0.2.0
## [0.1.3] - 2026-10-10

### 🐛 Bug Fixes

- Litestar plugin lack lifespan

### 🚜 Refactor

- Switch logging engine to structlog

### 📚 Documentation

- Align latest vesion v0.1.2

### 🧪 Testing

- Route structlog through stdlib logging so caplog captures records

### ⚙️ Miscellaneous Tasks

- Upgrade to v0.1.3
## [0.1.2] - 2026-10-10

### 🚀 Features

- Retry failed messages and cap queue capacity
- Defer failed redeliveries by a backoff

### 🐛 Bug Fixes

- Fail task mode arg
- Type-check error
- Type-check error
- Type-check error

### 🚜 Refactor

- Drop the queue capacity limit
- [**breaking**] Carry presence heartbeats over ipc

### 📚 Documentation

- Align the documented release with the package version
- Add task mode and scheduling documentation
- Correct stale examples in the quick start
- Describe the scheduler and per-mode submission in the architecture
- Correct the examples and architecture in the READMEs
- Correct docstrings rendered by autodoc
- Document retries, queue capacity and the enqueued_at move
- Describe the backoff and the disabled capacity limit
- Remove the queue capacity limit description
- Describe the ipc presence heartbeat and drop redis_dsn
- Add a roadmap of the known gaps

### 🧪 Testing

- Cover the capacity limit, bounded retries and the enqueued_at move
- Follow the deferred redelivery in the worker cases
- Drop the queue capacity limit tests
- Rework the presence suite for the ipc heartbeat

### ⚙️ Miscellaneous Tasks

- Capitalize the author name in project metadata
- Add the 0.1.1 changelog entry
- Upgrade to v0.1.2
## [0.1.1] - 2026-10-08

### 🚀 Features

- Move queue ownership to TaskSpec
- Add delay and cron scheduling via APScheduler
- Task samples
- Add interval mode to task declarations
- Register interval tasks on the client scheduler
- Add an interval sample task
- Add a task scheduler for delay, cron and interval modes

### 🐛 Bug Fixes

- Spawn worker subprocesses instead of forking
- Correct the task timeout budget
- Bound worker shutdown and stabilize the broker client
- Re-stamp enqueued_at when re-delivering a reclaimed message

### 🚜 Refactor

- Schedule client tasks through the task scheduler

### 🧪 Testing

- Align the unit suite with the queue contract
- Cover the master supervision loop
- Isolate integration tests per pipeline
- Cover the task scheduler
- Scheduler pipeline integration

### 📦 Build System

- Point pytest at the repository root and waive APScheduler stubs

### ⚙️ Miscellaneous Tasks

- Upgrade to version 0.1.1
## [0.1.0] - 2026-10-06

### 🚀 Features

- Base constructions
- Task module impl
- Broker protocol with redis impl
- Binomic worker
- Worker heartbeat tick append timestamp
- Binomic worker and master impl
- Binomic client
- Remove unused dependencies
- Example tasks
- Broker protocol and redis broker impl
- Binomic plugin for litestar
- Binomic client with factory and config
- Makefile within changelog

### 🐛 Bug Fixes

- Mypy check error
- Makefile test integration command

### 🚜 Refactor

- Task decorator rebuild
- Binomic worker rebuild

### 📚 Documentation

- Changelog
- Add bilingual README with cross-links
- Add bilingual contributing guide with AI-assisted code policy
- Document the maintainer release process
- Define release version naming rules
- Build bilingual Sphinx documentation site

### 🧪 Testing

- Unit tests
- Integration tests

### 📦 Build System

- Add MIT license and declare it in package metadata
- Add project URLs to package metadata

### ⚙️ Miscellaneous Tasks

- Test action

### 💼 Other

- Add tag-triggered draft release workflow
- Skip test pipeline for docs-only changes
