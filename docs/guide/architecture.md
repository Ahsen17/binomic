# 架构

```{mermaid}
flowchart LR
    P[生产者<br>Binomic 客户端] -->|submit：direct| B[AsyncredisBroker<br>Redis Streams]
    P -->|submit：delay| S[TaskScheduler]
    P -.->|启动时注册：cron / interval| S
    S -->|到期投递| B
    B -->|consumer group / PEL| W1[Worker 0<br>子进程]
    B -->|consumer group / PEL| W2[Worker 1<br>子进程]
    M[Master<br>监督进程] --> W1
    M --> W2
    M -->|presence 心跳| R[(Redis)]
```

## 组件

- **Producer**：`Binomic.submit` 按任务的模式分支处理 —— `direct` 任务立即经
  `BrokerFactory` 创建代理并 `enqueue` 消息；`delay` 任务交给调度器延后投递；
  `cron` 与 `interval` 任务**不经** `submit`（对其调用会抛 `ValueError`），而是在
  客户端启动时按声明注册为周期作业。消息进哪条 Stream 取自任务声明
  （`TaskSpec.queue`），不由调用方指定。
- **Scheduler**：`TaskScheduler` 为 `delay` / `cron` / `interval` 三种模式创建对应的
  触发器，到期后走与 `direct` 相同的入队路径。详见[任务模式与调度](scheduling.md)。
- **Broker**：`AsyncredisBroker` 将消息写入 Redis Streams，Worker 侧以消费者组
  读取；失联消费者的 PEL 消息由 `reclaim`（`xautoclaim`）回收重投。
- **Master / Worker**：Master 以 multiprocessing 拉起 Worker 子进程并监督其存活；
  Worker 在进程内通过 `anyio` 以配置并发执行任务，执行结果经 `ack` 确认。
