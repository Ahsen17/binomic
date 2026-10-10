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

第一个位置参数是**队列名**，需与客户端配置中的 `queues` 对应。任务以**函数名的小写形式**
注册（上例中为 `example`、`delayed`、`periodic`、`poll`），投递时用它来指定要执行的任务。

`cron` 与 `interval` 两种周期模式的函数**不能带参数** —— 周期触发时没有调用参数可传，
声明时会直接抛 `ValueError`。四种模式的完整说明见[任务模式与调度](scheduling.md)。

## 启动并投递

```python
from binomic.client import Binomic, BinomicFactory
from binomic.config import BinomicConfig
from binomic.message import Message
from binomic.task import autodiscover

# 发现 myapp 包内所有 tasks.py 模块并注册任务
autodiscover("myapp")

factory = BinomicFactory(
    broker_dsn="redis://localhost:6379/0",
    redis_dsn="redis://localhost:6379/1",
    module_name="myapp",
    config=BinomicConfig(queues=["default"], workers=2, concurrency=5),
)

binomic: Binomic = factory.create()

# 进入上下文后启动 Master（拉起 worker 子进程）并提供提交入口
async with binomic:
    await binomic.submit(Message(name="example", args=[1]))
```

- `broker_dsn` 指向用于投递消息的 Redis Streams；
- `redis_dsn` 指向用于 presence 心跳与监督协调的 Redis；
- `BinomicConfig` 中 `workers` 是 Master 拉起的 Worker 子进程数量，
  `concurrency` 是每个 Worker 并发执行的任务数，`max_attempts` 是单条消息的最大尝试
  次数 —— 其语义见[可靠性](reliability.md)；
- `Message` 的 `name` 是任务名，`args` / `kwargs` 承载调用参数（上例会以 `index=1`
  调用 `example`）。消息进到哪条 Stream 由**任务声明里的队列**决定，消息上不需要
  也不能指定队列；
- `enqueued_at` 无需手工赋值：它在**实际投递时**由客户端写入该消息的 Stream entry
  （不是 `Message` 上的字段），延迟与周期任务也因此每次投递都拿到当前时间。
