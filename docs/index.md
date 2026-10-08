# Binomic

基于 [anyio](https://anyio.readthedocs.io/) 与 Redis Streams 的异步任务框架。

Binomic 以装饰器声明任务，以 Redis Streams 投递消息，由多进程 Worker 消费执行，
Master 进程负责监督与故障回收。消息体采用
[msgspec](https://jcristharif.com/msgspec/) JSON 序列化，任务 ID 使用 UUIDv7。

## 特性

- **声明式任务**：`@task()` 装饰器，支持 `direct`（立即）、`delay`（延迟）、
  `cron`（cron 表达式）、`interval`（固定间隔）四种模式，详见
  [任务模式与调度](guide/scheduling.md)
- **Redis Streams 消息代理**：基于消费者组与 PEL 的可靠投递，支持通过
  `xautoclaim` 回收失联消费者的消息
- **多进程 Worker**：Master 以 multiprocessing 启动多个 Worker 子进程，并携
  presence 心跳持续监督
- **类型安全**：全量 mypy strict 与 ruff 检查，任务注册表基于泛型 `TaskSpec[P, T]`
- **Litestar 集成**：内置 `BinomicPlugin`，将 Binomic 客户端注入 Litestar 依赖

要求 Python ≥ 3.12，Redis ≥ 7.x。

```{toctree}
:hidden:

guide/quickstart
guide/architecture
guide/scheduling
guide/litestar
api
```
