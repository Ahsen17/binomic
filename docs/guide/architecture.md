# 架构

```{mermaid}
flowchart TD
    P[生产者<br>Binomic 客户端] -->|submit：direct| B[AsyncredisBroker<br>Redis Streams]
    P -->|submit：delay| S[TaskScheduler]
    P -.->|启动时注册：cron / interval| S
    S -->|到期投递| B
    B -->|consumer group / PEL| W1[Worker 0<br>子进程]
    B -->|consumer group / PEL| W2[Worker 1<br>子进程]
    W1 -.->|失败或超时：退避后重投<br>attempt + 1| B
    M[Master<br>监督进程] -->|判死则重启| W1
    M -->|判死则重启| W2
    W1 -.->|presence 心跳：IPC| M
    W2 -.->|presence 心跳：IPC| M
```

## 组件

- **Producer**：`Binomic.submit` 按任务的模式分支处理 —— `direct` 任务立即经
  `BrokerFactory` 创建代理并 `enqueue` 消息；`delay` 任务交给调度器延后投递；
  `cron` 与 `interval` 任务**不经** `submit`（对其调用会抛 `ValueError`），而是在
  客户端启动时按声明注册为周期作业。消息进哪条 Stream 取自任务声明
  （`TaskSpec.queue`），不由调用方指定。
- **Scheduler**：`TaskScheduler` 为 `delay` / `cron` / `interval` 三种模式创建对应的
  触发器，到期后走与 `direct` 相同的入队路径。每个 Worker **自己**也持有一个
  `TaskScheduler`（与客户端那个互不相干），专门承担失败重投的退避延时。
  详见[任务模式与调度](scheduling.md)。
- **Broker**：`AsyncredisBroker` 将消息写入 Redis Streams，Worker 侧以消费者组
  读取；失联消费者的 PEL 消息由 `reclaim`（`xautoclaim`）回收重投，重投时 `attempt + 1`。
- **Master / Worker**：Master 以 multiprocessing 拉起 Worker 子进程并监督其存活。
  心跳走**进程间 IPC** 而非 Redis：Master 为每个 worker 建一个共享
  `multiprocessing.Value`，随 `Worker` 构造参数传给子进程，子进程按期写入时间戳、
  Master 直接读取判死。这样在 `is_alive()`（覆盖「进程已死」）之外，还能识别
  「进程活着但已停跳」——后者是弱化掉心跳就检测不到的那一路。Worker 在进程内通过
  `anyio` 以配置并发执行任务。任务成功后 `ack`；失败或超时则**退避后**
  在 `max_attempts` 之内重投（退避经 Worker 自有的调度器延时投递），超出后记日志丢弃；
  任务被取消时**不 ack**，消息留给 `reclaim`。详见[可靠性](reliability.md)。
