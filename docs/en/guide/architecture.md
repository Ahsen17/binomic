# Architecture

```{mermaid}
flowchart LR
    P[Producer<br>submit] --> B[AsyncredisBroker<br>Redis Streams]
    B -->|consumer group / PEL| W1[Worker 0<br>subprocess]
    B -->|consumer group / PEL| W2[Worker 1<br>subprocess]
    M[Master<br>supervisor] --> W1
    M --> W2
    M -->|presence heartbeat| R[(Redis)]
```

## Components

- **Producer**: `Binomic.submit` creates a broker through `BrokerFactory`
  based on the broker DSN scheme and enqueues the message.
- **Broker**: `AsyncredisBroker` writes messages to Redis Streams; workers
  read them through a consumer group, and messages in the PEL of a lost
  consumer are reclaimed and redelivered by `reclaim` (`xautoclaim`).
- **Master / Worker**: the master spawns worker subprocesses through
  multiprocessing and supervises their liveness; each worker executes tasks
  concurrently with `anyio` inside its process and acknowledges results
  with `ack`.
