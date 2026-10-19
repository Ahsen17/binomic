# 任务模式与调度

任务在**声明时**确定模式，模式决定它何时被执行：是提交即执行，还是延后一次，
还是按周期反复执行。共四种模式 —— `direct`、`delay`、`cron`、`interval`。
它们在提交链路上的位置见[架构](architecture.md)。

## 声明任务

```python
from binomic.task import task


@task("default")
def example(index: int = 0) -> None: ...
```

第一个位置参数是**队列名**，需与 `BinomicConfig` 的 `queues` 对应；`mode` 是仅限
关键字传参，默认 `"direct"`。任务以**函数名原样**注册（区分大小写），投递时用该名字指定任务；
注册表内名字唯一，重复注册同名任务会抛 `DuplicateTaskError`。每个任务还可以声明
`on_dead` 死信通知钩子（`@task(..., on_dead=fn)`）：消息因超时、超龄或耗尽
`max_attempts` 进入 DLQ 后，`fn(entry, reason, exc)` 会被调用一次——契约见
[可靠性](reliability.md)。

| 模式 | 附加参数 | 执行时机 | 能否带参数 |
|-|-|-|-|
| `direct` | 无（默认） | 提交时立即执行 | 可以 |
| `delay` | `delay`：延迟秒数 | 提交后延迟指定秒数执行一次 | 可以 |
| `cron` | `cron`：cron 表达式字符串 | 按 cron 表达式重复执行 | **不可以** |
| `interval` | `interval`：间隔秒数 | 每隔指定秒数重复执行 | **不可以** |

```python
# 延迟 10 秒执行一次
@task("default", mode="delay", delay=10)
def delayed() -> None: ...


# 每 5 分钟执行（cron 表达式，分/时/日/月/周）
@task("default", mode="cron", cron="*/5 * * * *")
def periodic() -> None: ...


# 每 30 秒执行
@task("default", mode="interval", interval=30)
def poll() -> None: ...
```

声明期会立刻校验，不合法直接抛 `ValueError`：

- 缺必需参数：`delay` / `cron` / `interval` 模式各自要求对应的参数；
- 周期任务带参数：`cron` 与 `interval` 模式的函数**不能有参数** —— 周期触发时没有调用
  参数可传（原因见下节）。

延迟与间隔的取值只要求是**正数**，这条在**声明期**由装饰器校验（`delay` / `interval`
`<= 0` 直接抛 `ValueError`）。`TaskScheduler` 本身是通用的可调用对象封装、不感知任务
语义，因此不做这类校验，见下文「直接使用调度器」。

```{note}
`cron` 与 `interval` 的触发时刻按**调度器时区**计算，默认是 **UTC**。客户端自动注册
周期任务时不接受时区配置，因此 `cron="0 9 * * *"` 指的是 **UTC 9 点**而非本地 9 点。
需要按其它时区触发时，用下面的 `TaskScheduler(timezone=...)` 自行注册。
```

## 提交任务

`Binomic.submit` 把一条 `Message` 交给客户端处理，但**只支持两种模式**：

- `direct`：立即把消息写入任务声明的队列；
- `delay`：交给调度器，延迟到期后再写入队列。

对 `cron` 或 `interval` 任务调用 `submit` 会抛 `ValueError` —— 周期任务不接受单次
提交，它们的触发由客户端在启动时自动注册（见下节）。消息进哪条 Stream 由**任务声明
里的队列**决定，`submit` 不接受调用方指定队列。

```python
from binomic.broker import Message

# `binomic` 的构造见「快速上手」
async with binomic:
    await binomic.submit(Message(task="example", args=[1]))
```

## 周期任务的注册

客户端在进入上下文（`async with binomic`）时，遍历注册表中所有 `cron` / `interval`
任务，为每个任务注册一个周期作业：每次触发即向该任务的队列投递一条消息。这也解释了
上一节的约束 —— 周期触发的消息不带调用参数，因此周期任务必须是无参函数。

## 直接使用调度器

`TaskScheduler` 是公开 API，可独立于客户端使用。

```python
import asyncio

from binomic.task import TaskScheduler


def send_reminder(user_id: str) -> None:
    print(f"reminder for {user_id}")


async def main() -> None:
    scheduler = TaskScheduler()
    scheduler.start()  # 必须在运行中的事件循环内
    scheduler.delay(func=send_reminder, delay=5.0, args=("u-1",))

    await asyncio.sleep(6)  # 等作业到期
    scheduler.close()


asyncio.run(main())
```

`TaskScheduler` 只接收可调用对象，不感知 `TaskSpec`；延时、cron 表达式、间隔都由调用
方直接给出，到期后原样调用 `func`。

- **必须在运行中的事件循环内使用**：`TaskScheduler` 基于 APScheduler 的
  `AsyncIOScheduler`，后者在 `start()` 时就地捕获当前事件循环 —— 在事件循环之外调用
  `start()` 会抛 `RuntimeError: no running event loop`。三个方法本身是同步方法，
  但前提是有一个正在运行的事件循环（客户端路径天然满足，因为它在事件循环内启动调度器）。
- **构造**：`TaskScheduler(timezone=UTC)`，默认时区为 UTC；三个方法创建的触发器都使用
  该时区。
- **方法**：`delay(func, *, delay, args=None, kwargs=None)`、
  `cron(func, *, cron, args=None, kwargs=None)`、
  `interval(func, *, interval, args=None, kwargs=None)`，均为**同步方法**；`func` 是
  到期时要调用的可调用对象，`args` / `kwargs` 原样传给 `func`。调度器不感知任务语义，
  时序值不合法时由 APScheduler 的触发器构造报错。
- **校验归属**：任务语义（模式、必填参数、正数、`on_dead` 须为可调用对象）在
  **声明期**由 `@task` 装饰器校验，在**提交期**由 `Binomic.submit` 复核；调度器只
  负责排程。Worker 的失败重投就是直接调 `scheduler.delay` 把 `broker.retry` 排成
  延时作业（见[可靠性](reliability.md)）。
- **生命周期**：`start()` 之后作业才会真正触发，`close()` 停机，二者均为同步方法；
  `close()` 对底层 `AsyncIOScheduler` 的停止在事件循环的下一个 tick 生效。注意
  `close()` 紧跟在调度之后调用会让作业来不及触发（上例用 `await asyncio.sleep(6)`
  等到达期）。
- **迟到也执行**：`delay` 排出的作业带 `misfire_grace_time=None`，因此即便调度器在
  `run_date` 之后才轮到它（事件循环被占住、或进程一度停顿），作业照样触发，不会被当作
  「错过的运行」而静默丢弃 —— 延后投递代表的是**必须发生**的动作；`cron` / `interval`
  沿用 APScheduler 默认宽限。
