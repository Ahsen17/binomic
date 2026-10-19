# 可靠性

一条消息投出去之后可能执行失败、超时或滞留。这一页说明各类情形的处置：
**失败重试**、**超时与超龄**，以及**死信队列（DLQ）**。它们在链路中的位置见
[架构](architecture.md)。

## 尝试次数与时限

`Message` 带一个 `attempt` 字段（默认 `1`），随 payload 一起序列化，跨进程、跨重投
都跟着消息走。`task_timeout` 则是一条消息的**投递总时限**：从首次入队起算，重投不
重置。两者配合给出每条消息的处置规则。

- 客户端首次投递：`attempt = 1`；
- Worker 执行失败：`attempt` 由 broker 侧 Lua 脚本 +1，随后退避重投；
- `reclaim` 回收宕机 worker 滞留的消息：`attempt` +1，且**重新盖戳**（见下文）。

## 失败重试

任务抛异常时，Worker 按以下顺序处理：

1. 记一条失败日志；
2. 若 `attempt + 1 > max_attempts`（默认 `3`），消息进**死信队列**，流程结束；
3. 否则经 Worker 自有的调度器按**退避**延时调用 broker 的原子重投脚本——它把
   「写入副本」与「ack 原件」合并为一次 Lua 调用，副本的 `attempt` 已 +1，而
   `enqueued_at` **保留原值**（时限跨重投累计，见下文）。

```{note}
重投**不是**立即发生的：退避首跳 1.5 秒，逐次翻倍（3 / 6 / 12 / 24 秒），封顶
**30 秒**，由 Worker 自己持有的调度器承担，并且「迟到也执行」——不会因为事件循环
繁忙而被静默跳过。退避间隔目前固定，不可配置；需要别的节奏时，请在任务函数内部
自行等待。
```

## 超时与超龄（直送 DLQ）

`task_timeout` 在两处生效，语义都是「投递总时限」：

- **执行超时**：任务执行超过 `task_timeout` 被取消——消息**直接进 DLQ，不重试**；
- **超龄**：消息的 `enqueued_at` 距今已超过 `task_timeout`（排队太久）——不执行，
  直接进 DLQ。

失败重投**不重置**时限：副本保留原 `enqueued_at`，所以「排队 + 执行 + 退避」累计
超过 `task_timeout` 的消息，会在下次投递时被超龄判定送入 DLQ。唯一的例外是
`reclaim`（见下文）。

## 死信队列（DLQ）

进入 DLQ 的三类消息：执行超时、排队超龄、`max_attempts` 耗尽。DLQ 是每个队列一对
键——`{namespace}:{queue}:dlq:stream` + `:dlq:hash`；副本的 `attempt` 与最后一次
投递一致（终态不再加一），原始 payload 原样保留，可检查、可重放。

## 死信通知钩子（on_dead）

DLQ 不是终点的静默：任务可以用 `@task(..., on_dead=fn)` 声明一个**死信通知钩子**。
消息因上述三条路径进入 DLQ 后，Worker 会在 DLQ 写入完成之后调用一次 `fn`，把消息
上下文交给你：

```python
from binomic.broker import MessageEntry
from binomic.task import OnDeadReason, task


def alert(
    entry: MessageEntry,
    reason: OnDeadReason,
    exc: BaseException | None,
) -> None:
    print(f"[{reason}] {entry.message.task} on {entry.queue} is dead")


@task("default", on_dead=alert)
async def charge() -> None: ...
```

钩子契约：

- 以 `fn(entry, reason, exc)` **位置参数**调用：`entry` 是消费侧封套 `MessageEntry`
  （队列、stream 条目 id、payload、投递时间戳俱全）；`reason` 是 `"timeout"` /
  `"expired"` / `"max_attempts"` 之一；`exc` 为触发异常——执行超时是 `TimeoutError`
  实例，耗尽次数是任务抛出的原始异常，超龄则是 `None`；
- **无返回值**：钩子是对既成事实的通知，不是控制流——返回值被忽略；从 DLQ 重放等
  后续处置由消费方自行承担；
- 同步、异步函数皆可；
- **永不影响投递语义**：钩子抛出的异常会被记录（warning）并吞掉，消费循环照常；
  唯有取消（与退出信号）原样穿透；
- **best-effort 解析**：钩子挂在任务声明上，随同一条 `autodiscover` 链路到达
  Worker；消息对应的任务名未注册时钩子被跳过，DLQ 写入不受影响；
- **钩子必须快**：它运行在 `task_timeout` 之外、没有超时预算，挂死的钩子会占住一个
  并发槽。

类型别名 `OnDeadReason` 与 `OnDeadHook` 从 `binomic.task` 导出，可为钩子函数写注解。

## reclaim：宕机恢复

worker 进程死亡（崩溃、被杀）时，其 PEL 里的消息无人 ack。其它消费者每
`reclaim_interval` 跑一次 `XAUTOCLAIM`，把滞留超过 `min_idle_ms`（即 `task_timeout`
的毫秒数，下限 10 秒）的消息重投给自己。与失败重投不同，reclaim 的副本**重新盖戳**：
worker 死亡属于「投递丢失」而非「任务失败」，副本获得完整的新时限窗口——否则滞留
时间本身已超龄，回收将永远到货即死。`attempt` 照常 +1。

`XAUTOCLAIM` 之外，同一次脚本还会做一遍**消费者元数据清扫**：consumer 名带
hostname + Master pid 前缀（同主机与跨主机实例均不撞名），实例扩缩容会在消费组里留下死 consumer；
**PEL 为空**且闲置超过 `gc_idle_ms`（由 policy 派生，下限为十个 `reclaim_interval`）
的会被清除。PEL 非空的 consumer 绝不动——`DELCONSUMER` 会销毁其未 ack 条目，让
那些消息永久滞留。

## 不重试、不进 DLQ 的两类

- 任务名未注册（`TaskNotFoundError`）：重投同样的 payload 不会改变结果，消息被
  `ack` 后丢弃；
- 任务被取消、或 worker 收到退出信号（`SystemExit` / `KeyboardInterrupt`）：异常
  原样上抛，消息**不 ack**，留在 PEL 里等下一次 `reclaim` 交给其它消费者。

payload 的完整性由 broker 层把守：`acquire` / `reclaim` 遇到读不出负载的条目直接
清理（`XACK` + `XDEL`），这类损坏不计入重投。
