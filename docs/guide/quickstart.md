# 快速上手

## 安装

```bash
# uv
uv add binomic

# pip
pip install binomic
```

## 声明任务

在应用包内的 `tasks.py` 模块中用装饰器声明任务（Worker 通过
`autodiscover` 自动发现所有 `tasks.py` 模块）：

```python
# myapp/tasks.py
import time

from binomic.task import task


@task("default")
def example(index: int = 0) -> None:
    print(f"[{index}] Current time: {time.time()}")


@task("default", mode="delay", delay=10)  # 延迟 10 秒执行
def delayed() -> None: ...


@task("default", mode="cron", cron="*/5 * * * *")  # 每 5 分钟执行
def periodic() -> None: ...


@task("default", mode="interval", interval=30)  # 每 30 秒执行
def poll() -> None: ...
```

第一个位置参数是**队列名**，需与客户端配置中的 `queues` 对应。任务以**函数名原样**
注册（区分大小写，上例中为 `example`、`delayed`、`periodic`、`poll`），投递时用它来
指定要执行的任务。

`cron` 与 `interval` 两种周期模式的函数**不能带参数** —— 周期触发时没有调用参数可传，
声明时会直接抛 `ValueError`。四种模式的完整说明见[任务模式与调度](scheduling.md)。

## 启动并投递

```python
from binomic.client import Binomic, BinomicConfig, BinomicFactory
from binomic.broker import Message

factory = BinomicFactory(
    broker_dsn="redis://localhost:6379/0",
    module_name="myapp",
    config=BinomicConfig(queues=["default"], workers=2, concurrency=5),
)

binomic: Binomic = factory.create()

# 进入上下文后启动 Master（拉起 worker 子进程）并提供提交入口；
# 构造时已自动发现 myapp 包内所有 tasks.py 模块并注册任务
async with binomic:
    await binomic.submit(Message(task="example", args=[1]))
```

- `broker_dsn` 是**唯一**需要提供的 Redis 连接：消息投递与消费都走它指向的
  Redis Streams；Master 对 Worker 的存活监督走进程间 IPC，不需要 Redis；
- `BinomicConfig` 中 `workers` 是 Master 拉起的 Worker 子进程数量，
  `concurrency` 是每个 Worker 并发执行的任务数，`max_attempts` 是单条消息的最大尝试
  次数，`capacity` 是所有队列共享的未确认总数上限（默认 `None`，不限流）——
  语义均见[可靠性](reliability.md)；`task_timeout`（投递总时限，秒）、
  `read_count` 与 `poll_interval`（拉取循环）、`heartbeat_interval` 与
  `reclaim_interval`（心跳与崩溃回收节奏）同样是它的字段，默认值见 API 参考；
- `Message` 的 `task` 是任务名，`args` / `kwargs` 承载调用参数（上例会以 `index=1`
  调用 `example`）。消息进到哪条 Stream 由**任务声明里的队列**决定，消息上不需要
  也不能指定队列；
- Redis key 前缀与消费者组名（`namespace`）由客户端与 Worker 各自按 `module_name`
  在内部派生，不是用户面参数：同一 `module_name` 的客户端与 Worker 自动共用命名空间；
- `enqueued_at` 无需手工赋值：它在**实际投递时**由客户端进程内的 broker 在
  enqueue 时盖戳（不是 `Message` 上的字段），延迟与周期任务也因此每次投递都拿到
  当前时间；
- 可观测性由 `otel_enabled` 开启、`worker_init` 负责在 worker 子进程内安装 SDK，
  详见[可观测性](observability.md)。
