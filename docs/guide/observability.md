# 可观测性

Binomic 产出可观测性数据：**traces 与 metrics** 通过 OpenTelemetry（OTel）API
发射，日志由 structlog 输出、可由应用桥接进 OTel。采集是**可选能力**：OTel 依赖
放在独立 extra 中，且默认关闭。Binomic 只负责产生
遥测数据，**SDK、provider 与 exporter 全部由调用方装配** —— 库内不注册任何
exporter，也不会向 collector 输出。

## 安装与启用

```bash
# uv
uv add "binomic[otel]" opentelemetry-sdk

# pip
pip install "binomic[otel]" opentelemetry-sdk
```

`binomic[otel]` 只包含 `opentelemetry-api`；下面的示例还导入 `opentelemetry.sdk`，
SDK 与具体 exporter 需按你的后端另行安装。

```python
from binomic.client import BinomicConfig

config = BinomicConfig(
    queues=["default"],
    otel_enabled=True,
)
```

`otel_enabled` 默认 `False`：此时 Binomic 不做任何 OTel 调用、不产生 span/metric，
也不在 Redis 中写 trace 上下文，行为与未引入可观测性时完全一致。structlog 日志
**不**受该开关控制，始终按配置输出。

启用了却未安装 extra 时，构造 `Binomic` 会立即抛出带安装提示的 `RuntimeError`
（fail-fast），不会静默降级；standalone `Master` 在构造时同样校验。

### Litestar

`BinomicPlugin` 的第三个参数是完整的 `BinomicConfig`，可观测性配置随它传入即可：

```python
from binomic.client import BinomicConfig
from binomic.plugin.litestar import BinomicPlugin
from myapp.telemetry import init_worker

plugin = BinomicPlugin(
    app_name="myapp",
    broker_dsn="redis://localhost:6379/0",
    config=BinomicConfig(
        queues=["default"],
        otel_enabled=True,
        worker_init=init_worker,
    ),
)
```

插件不会替你初始化 SDK：Litestar 进程（客户端侧）的 provider 由应用在启动前自行设置；
worker 子进程侧由 `worker_init` 负责。两处都按上文的方式配置即可。

## Traces

### 应用进程（客户端）

在构造 `Binomic` **之前**设置全局 provider。SDK 安装方式由你决定：

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
```

> 每个进程只能设置一次全局 provider；再次设置只会告警且不生效。

每次 `submit`（以及 cron/interval 的每次触发）产生一个 `create {queue}` PRODUCER
span，并把当前 W3C trace context（`traceparent`、`tracestate`、`baggage`）写入
broker 的独立上下文存储（Redis 实现为 `{namespace}:{queue}:context`，按消息 id
存放）—— 消息封套（`Message` / `MessageEntry`）本身不携带任何遥测字段。direct
任务在本次 enqueue 时写入；delay 任务在提交时捕获上下文、到期投递时写入。

### Worker 子进程

Worker 由 Master 以 **spawn** 方式拉起，子进程不会继承父进程的 provider。
在子进程内安装 SDK 的唯一入口是 `worker_init`：

```python
# myapp/telemetry.py
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider


def init_worker() -> None:
    provider = TracerProvider()
    trace.set_tracer_provider(provider)
```

```python
from binomic.client import BinomicConfig
from myapp.telemetry import init_worker

config = BinomicConfig(
    queues=["default"],
    otel_enabled=True,
    worker_init=init_worker,
)
```

- `worker_init` 在每个 worker 子进程启动时、事件循环创建前**同步调用一次**；
  它必须是同步可调用对象——async 函数的返回协程不会被等待；
- 它必须是**模块级函数**（spawn 会按引用序列化），不能是 lambda 或闭包；
- pickle 传递的是 `模块名:限定名` 的引用而不是代码，因此该模块在 worker 子进程里
  **必须可导入**（与主进程同一环境、同一包路径）；
- 回调抛异常时子进程直接退出，由 Master 的失联检测重启——持续失败会表现为持续
  重启，便于暴露配置错误。

执行侧产生 `process {task}` CONSUMER span，其父上下文从提交时写入的存储中按
消息 ID 读取；上下文缺失（例如旧消息）时自动降级为 root span。消费端采用
parent-child 关联（单消息、无 ambient span 场景，与 Celery 等实现一致）。

### 语义约定

Span 名与属性遵循 OpenTelemetry messaging 语义约定的形状：
`messaging.system=binomic`、`messaging.operation.name=create|process`、
`messaging.destination.name=<queue>`、`messaging.message.id=<uuid>`。自定义属性
使用 `binomic.` 前缀。`traceparent`、`tracestate` 与 `baggage` 均随消息传播。

## Metrics

所有指标使用 `binomic.` 命名空间，单位遵循 UCUM：

| 指标 | 类型 | 单位 | 含义 |
|-|-|-|-|
| `binomic.task.execution.duration` | Histogram | `s` | 任务函数执行墙钟时长 |
| `binomic.task.delivery.lag` | Histogram | `s` | 投递总滞留：执行开始 − `enqueued_at` |
| `binomic.task.submissions` | Counter | `{task}` | 提交尝试次数（`enqueue` 的静默降级不计入失败） |
| `binomic.task.rejections` | Counter | `{task}` | 容量拒绝次数（属性 `binomic.task.name` 与 `messaging.destination.name`；受 `otel_enabled` 门控） |
| `binomic.task.executions` | Counter | `{task}` | 执行次数（含每次重投） |
| `binomic.task.acks` | Counter | `{task}` | ack 次数 |
| `binomic.task.retries` | Counter | `{task}` | 退避重投次数 |
| `binomic.task.dead_letters` | Counter | `{task}` | 进 DLQ 次数（带 `reason`） |
| `binomic.task.reclaims` | Counter | `{task}` | reclaim 回收条数 |
| `binomic.worker.inflight.tasks` | UpDownCounter | `{task}` | 在途任务数（按进程） |
| `binomic.worker.restarts` | Counter | `{worker}` | visor 判死重启次数（属性 `binomic.worker.reason`：`heartbeat` = 进程活着但心跳停滞、`dead` = 进程死亡） |
| `binomic.worker.heartbeat.age` | ObservableGauge | `s` | 每个 worker 的心跳年龄（从未起跳呈极大值） |
| `binomic.queue.depth` | ObservableGauge | `{message}` | stream 长度（含未确认） |
| `binomic.queue.oldest_pending.age` | ObservableGauge | `s` | 最老 entry 的年龄 |

**关于 `delivery.lag`**：它锚定的是消息的**投递总时限**戳 `enqueued_at`。失败重投
保留旧戳，因此一条重试过的任务其 lag 会累计历史等待——这是定义使然（投递总滞留），
不是每次投递的单独延迟。

直方图的显式桶由调用方的 View 配置决定，Binomic 不强制；可直接使用 OTel
semconv 的秒级标准桶集合。

### 监督（Master）

监督指标由 **Master**（客户端进程内的监督循环）发射，走客户端进程的 provider，
与 `worker_init` 无关：

- `binomic.worker.restarts` 在 visor 判死重启时 +1；`close()` 的正常停机不计入，
  告警侧无需排除停机窗口。`close()` 亦不清理心跳 cells——客户端形态下进程随即
  退出；standalone 进程若在 `close()` 后继续存活，age 会无界增长且不再产生
  重启计数；
- `binomic.worker.heartbeat.age` 按 worker 报告「当前时刻 − 最后心跳」。从未起跳
  的 worker 呈现为极大年龄——语义如此：从未起跳即最陈旧，阈值告警自然覆盖慢启动；
- 重启计数与心跳年龄即告警两要素：「重启率突增」看 counter，「有 worker 失联」
  看 age 超阈值；「谁、何时、为何」由指标属性关联 structlog 的 `Worker restarted`
  日志回答。

心跳年龄 gauge 是**进程级全局注册**，同名 gauge 首次注册生效——一个进程运行一个
Master（客户端内建的形态）即语义正确。

## Logs

Binomic 通过 `structlog.stdlib.get_logger` 输出结构化日志，**不**内置任何日志配置
（库不应覆盖宿主应用的日志设置）。把日志送进 OTel 由你在应用侧桥接——Python 的
Logs API 仍处于 Development 阶段，桥接方式可能随版本演进：

```python
import logging

from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler

logger_provider = LoggerProvider()
logging.getLogger().addHandler(
    LoggingHandler(level=logging.INFO, logger_provider=logger_provider),
)
```

若使用 structlog 的 stdlib `ProcessorFormatter`，上述 handler 即可同时收到
Binomic 与应用自身的日志。级别约定：逐消息事件（提交、执行开始/完成、重试调度）
为 `debug`，生命周期（客户端、worker、broker 启停）为 `info`，失败路径（执行失败、
超时、超龄、DLQ、worker 重启）为 `warning`。

## 自定义 broker

trace 上下文的传递与队列观测是**可选能力**。自定义 broker 只需实现
`BrokerTelemetryMixin` 的两个方法即可接入（`trace_context` 与 `observe`，不要求
Lua）：把上下文随消息存进中间件自己的元数据区（例如 AMQP/Kafka 的 message
headers），执行时按消息 ID 取回即可。

不实现时 binomic 自动降级，**不会报错**：消息照常投递与执行，process span 变为
root span，队列深度 gauge 不注册。客户端也只在 broker 实现该能力时才传入上下文。

## 多次运行与多进程

- 每个进程的全局 provider 只能设置一次；重复设置会告警且不生效；
- worker 子进程各自独立上报，指标是按进程序列，聚合口径由后端决定；
- 上下文捕获/读取、span 错误标注与 gauge 回调的失败会被吞掉并降级为 debug
  日志，不影响投递、执行或 ack；trace/metric 的创建与 span 开启依赖 OTel API
  的不抛异常契约，并由 `ensure_available()` 在启用时前置校验；
- `ensure_available()` 是唯一故意抛错的入口：启用却未安装 extra 时立即失败。
