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


@task()
def example(index: int = 0) -> None:
    print(f"[{index}] Current time: {time.time()}")


@task(mode="delay", delay=10)  # 延迟 10 秒执行
def delayed() -> None: ...


@task(mode="cron", cron="*/5 * * * *")  # 每 5 分钟执行
def periodic() -> None: ...
```

## 启动并投递

```python
import time

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
    await binomic.submit(
        Message(name="example", queue="default", enqueued_at=time.time())
    )
```

- `broker_dsn` 指向用于投递消息的 Redis Streams；
- `redis_dsn` 指向用于 presence 心跳与监督协调的 Redis；
- `BinomicConfig` 中 `workers` 是 Master 拉起的 Worker 子进程数量，
  `concurrency` 是每个 Worker 并发执行的任务数。
