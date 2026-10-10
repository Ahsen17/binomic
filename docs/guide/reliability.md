# 可靠性

一条消息投出去之后可能执行失败、超时，或者干脆投不进去。这一页说明三种情形的处置：
**失败重试**、**投递背压**，以及尚未落地的**死信队列（DLQ）**。它们在链路中的位置见
[架构](architecture.md)。

## 尝试次数

`Message` 带一个 `attempt` 字段（默认 `1`），它随 payload 一起序列化，因此跨进程、
跨重投都跟着消息走。每投递一次算一次尝试：

- 客户端首次投递：`attempt = 1`；
- Worker 执行失败或超时：`attempt += 1`，未超出上限则**退避后**重投；
- `reclaim` 把滞留消息重投给新消费者时同样 `attempt += 1`。

## 失败重试

任务抛异常、或执行时间超出 `task_timeout` 时，Worker 按以下顺序处理：

1. 记一条失败日志；
2. `attempt += 1`；
3. 若 `attempt <= max_attempts`（默认 `3`），按**退避**延时把消息重新投给队列 ——
   这次尝试在队列里是一条新 entry；
4. 若超出上限，记 `max attempts reached` 并丢弃该消息（当前止于日志，见下文 DLQ）。

消息在队列里滞留超过 `task_timeout`（即 `enqueued_at` 太旧）时走同一条重投路径：它不会
被执行，而是带着新的时间戳重新入队。

```python
from binomic.config import BinomicConfig

# 单条消息最多尝试 3 次（首次 + 2 次重投）
config = BinomicConfig(queues=["default"], max_attempts=3)
```

```{note}
重投**不是**立即发生的：Worker 按退避延时投递，首跳 1.5 秒，此后逐次翻倍
（3 / 6 / 12 / 24 秒），封顶 **30 秒**。退避由 Worker 自己持有的调度器承担 —— 它把
「重新入队」排成一个延时作业，而**原件在排程之后就已经 ack**。由此有两个后果：

- 退避窗口内 Worker 进程若退出，该次重试不会发生 —— 换来的代价是绝不重复投递；
- 该延时作业被排在「迟到也执行」的位置上，不会因为事件循环繁忙而被静默跳过。

退避间隔目前是固定值，不可配置；需要别的节奏时，请在任务函数内部自行等待。
```

有两类失败**不重试**，只记日志并 `ack`（消息被丢弃）—— 重投同样的 payload 不会改变结果：

- 消息无法反序列化（payload 损坏）；
- 任务名未注册（`TaskNotFoundError`）。

**取消不 ack**：Worker 关闭或任务被取消时，消息**留在** PEL 里而不是被 ack，交给下一次
`reclaim` 重投给其它消费者。

## 投递背压：队列容量上限（当前未生效）

```{warning}
这条判定**目前被临时关掉了**：`AsyncredisBroker._outofcapacity` 里留着一行带
`# TODO: there is a bug` 的 `return False`，因此 `enqueue` 永远不会抛
`QueueCapacityLimitError`，`queue_capacity` 现在是个没有效果的配置项。本节描述的是它
**启用后**的行为，关闭的原因见本节末尾。
```

`queue_capacity`（默认 `1000`）**本意**是给每条队列设一个容量上限。判定依据是消费者组
自己的记账 —— `pending`（已投递未确认）加上 `lag`（尚未投递）—— 达到上限时 `enqueue`
抛 `QueueCapacityLimitError`：

```python
from binomic.broker import QueueCapacityLimitError
```

启用后，不同调用路径对「队列已满」的处置并不相同：

| 路径 | 队列已满时 |
|-|-|
| 客户端投递（`submit`） | 异常在客户端内部被吞掉并记日志：**`submit` 不抛错、仍返回消息 ID**，调用方无法从返回值判断消息是否被丢弃，也不会重试 |
| Worker 重投（失败重试） | 同样记日志后丢弃 —— 该次重试不再发生 |
| `reclaim` 重投滞留消息 | 记警告并**保留** PENDING（不 ack），下一轮 `reclaim` 再试 —— 视作背压而非失败 |

```{note}
容量是**软上限**：投递侧（客户端与 Worker 重投）在满时丢弃消息并记日志，只有 `reclaim`
会把消息留到下一轮。容量判定读的是消费者组的记账，因此队列需先创建消费者组（即完成
broker 初始化）；组不存在时视为没有容量上限。
```

**为什么被关掉**：判定读的是消费者组的记账，而 `xinfo_groups` 在 Stream 键还不存在时会抛
`no such key`。`initialize()` 只为 broker 配置里的队列建键，所以 `enqueue` 一旦投到一个
**未列入配置**的队列（例如某个任务声明的队列不在 `queues` 里），就会连带崩掉。
先短路判定、等这处修好再启用。

## 死信队列（尚未实现）

`max_attempts` 耗尽，以及上面两类不重试的失败，目前都只记日志然后丢弃；代码中留有
`TODO: send to DLQ` 标记。在 DLQ 落地之前，这些消息**不可恢复** —— 需要保留时，请在任务
函数内部自行捕获异常并落盘或另投队列。
