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
关键字传参，默认 `"direct"`。任务以**函数名的小写形式**注册，投递时用该名字指定任务；
注册表内名字唯一，重复注册同名任务会抛 `DuplicateTaskError`。

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

延迟与间隔的取值只要求是**正数**，这条在**提交/调度时**才由调度器校验（声明期只校验
参数是否存在），见下文「直接使用调度器」。

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
from binomic.message import Message

# `binomic` 的构造见「快速上手」
async with binomic:
    await binomic.submit(Message(name="example", args=[1]))
```

## 周期任务的注册

客户端在进入上下文（`async with binomic`）时，遍历注册表中所有 `cron` / `interval`
任务，为每个任务注册一个周期作业：每次触发即向该任务的队列投递一条消息。这也解释了
上一节的约束 —— 周期触发的消息不带调用参数，因此周期任务必须是无参函数。

## 直接使用调度器

`TaskScheduler` 是公开 API，可独立于客户端使用。

```python
import asyncio

from binomic.task import TaskScheduler, task


@task("default", mode="delay", delay=5)
def reminder_delay() -> None: ...


def send_reminder(user_id: str) -> None:
    print(f"reminder for {user_id}")


async def main() -> None:
    scheduler = TaskScheduler()
    scheduler.start()  # 必须在运行中的事件循环内
    scheduler.delay(func=send_reminder, spec=reminder_delay, args=("u-1",))

    await asyncio.sleep(6)  # 等作业到期
    scheduler.shutdown()


asyncio.run(main())
```

上例中 `reminder_delay` 只用来提供「延迟 5 秒」这一时序参数与校验，真正被调用的可调用
对象是传给 `func` 的 `send_reminder`。

- **必须在运行中的事件循环内使用**：`TaskScheduler` 基于 APScheduler 的
  `AsyncIOScheduler`，后者在 `start()` 时就地捕获当前事件循环 —— 在事件循环之外调用
  `start()` 会抛 `RuntimeError: no running event loop`。三个方法本身是同步方法，
  但前提是有一个正在运行的事件循环（客户端路径天然满足，因为它在事件循环内启动调度器）。
- **构造**：`TaskScheduler(timezone=UTC)`，默认时区为 UTC；三个方法创建的触发器都使用
  该时区。
- **`spec` 从哪来**：`@task(...)` 装饰器的返回值就是一个 `TaskSpec`，它提供时序参数
  （`delay` / `cron` / `interval`）并参与校验。
- **方法**：`delay(func, spec, *, delay=None, args=None, kwargs=None)`、
  `interval(...)`、`cron(...)` 均为**同步方法**；`func` 是到期时要调用的可调用对象，
  `args` / `kwargs` 原样传给 `func`。`delay` 多一个可选关键字 `delay`：给出时直接用它
  作为延时秒数，并**跳过**「`spec` 必须是 `delay` 模式」那一条校验 —— Worker 的失败重投
  正是靠它，把一个非 delay 模式的 `spec` 排成延时作业（见[可靠性](reliability.md)）。
- **校验**：`spec` 的模式与所调方法不符、或缺少对应取值时抛 `ValueError`（如
  ``Task is not a delay task or lack `delay` value.``，仅当未显式给出 `delay` 时生效）。
  `delay` 与 `interval` 的取值 `<= 0` 时也抛 `ValueError`
  （``... must be greater than 0.``）；`cron` 没有非正值校验 —— cron 表达式不存在
  「非正值」的概念。
- **生命周期**：`start()` 之后作业才会真正触发，`shutdown()` 停机，二者均为同步方法。
  注意 `shutdown()` 紧跟在调度之后调用会让作业来不及触发（上例用
  `await asyncio.sleep(6)` 等到达期）。
- **迟到也执行**：排出的作业带 `misfire_grace_time=None`，因此即便调度器在 `run_date`
  之后才轮到它（事件循环被占住、或进程一度停顿），作业照样触发，不会被当作「错过的运行」
  而静默丢弃 —— 延后投递代表的是**必须发生**的动作。
