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
