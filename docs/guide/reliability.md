# 可靠性

一条消息投出去之后可能执行失败、超时，或者干脆投不进去。这一页说明三种情形的处置：
**失败重试**、**投递背压**，以及尚未落地的**死信队列（DLQ）**。它们在链路中的位置见
[架构](architecture.md)。

## 尝试次数

`Message` 带一个 `attempt` 字段（默认 `1`），它随 payload 一起序列化，因此跨进程、
跨重投都跟着消息走。每投递一次算一次尝试：

- 客户端首次投递：`attempt = 1`；
- Worker 执行失败或超时：`attempt += 1`，未超出上限则重投；
- `reclaim` 把滞留消息重投给新消费者时同样 `attempt += 1`。

## 失败重试

任务抛异常、或执行时间超出 `task_timeout` 时，Worker 按以下顺序处理：

1. 记一条失败日志；
2. `attempt += 1`；
3. 若 `attempt <= max_attempts`（默认 `3`），以**新的** `enqueued_at` 把消息重新入队 ——
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
重投是立即入队的，没有退避（backoff）间隔：持续失败的消息会以最大速度消耗完
`max_attempts`。需要退避时，请在任务函数内部自行等待。
```

有两类失败**不重试**，只记日志并 `ack`（消息被丢弃）—— 重投同样的 payload 不会改变结果：

- 消息无法反序列化（payload 损坏）；
- 任务名未注册（`TaskNotFoundError`）。

**取消不 ack**：Worker 关闭或任务被取消时，消息**留在** PEL 里而不是被 ack，交给下一次
`reclaim` 重投给其它消费者。

## 投递背压：队列容量上限

`queue_capacity`（默认 `1000`）给每条队列设一个容量上限。判定依据是消费者组自己的记账 ——
`pending`（已投递未确认）加上 `lag`（尚未投递）—— 达到上限时 `enqueue` 抛
`QueueCapacityLimitError`：

```python
from binomic.broker import QueueCapacityLimitError
```

不同调用路径对「队列已满」的处置并不相同：

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

## 死信队列（尚未实现）

`max_attempts` 耗尽，以及上面两类不重试的失败，目前都只记日志然后丢弃；代码中留有
`TODO: send to DLQ` 标记。在 DLQ 落地之前，这些消息**不可恢复** —— 需要保留时，请在任务
函数内部自行捕获异常并落盘或另投队列。
