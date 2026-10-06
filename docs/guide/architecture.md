# 架构

```{mermaid}
flowchart LR
    P[生产者<br>submit] --> B[AsyncredisBroker<br>Redis Streams]
    B -->|consumer group / PEL| W1[Worker 0<br>子进程]
    B -->|consumer group / PEL| W2[Worker 1<br>子进程]
    M[Master<br>监督进程] --> W1
    M --> W2
    M -->|presence 心跳| R[(Redis)]
```

## 组件

- **Producer**：`Binomic.submit` 经 `BrokerFactory` 按 broker DSN 协议创建代理并
  `enqueue` 消息。
- **Broker**：`AsyncredisBroker` 将消息写入 Redis Streams，Worker 侧以消费者组
  读取；失联消费者的 PEL 消息由 `reclaim`（`xautoclaim`）回收重投。
- **Master / Worker**：Master 以 multiprocessing 拉起 Worker 子进程并监督其存活；
  Worker 在进程内通过 `anyio` 以配置并发执行任务，执行结果经 `ack` 确认。
