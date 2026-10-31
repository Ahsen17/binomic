# assets — Redis HA 验证环境

`test_integration` 的 `ha` 套件（Sentinel / Cluster 拓扑兼容性验证，任务
1793440391）使用的本地验证环境。两套拓扑互相独立、可分别启停。

前置条件：

- WSL 发行版内原生 Docker（`docker compose version` ≥ 2.x）；两套环境都用
  host 网络模式，容器端口直接绑定在本机回环地址上。
- 以下端口未被占用：6380–6382、26380–26382（Sentinel）；7000–7005、
  17000–17005（Cluster）。

## Sentinel 拓扑（1 主 2 从 3 哨兵）

```bash
docker compose -f assets/sentinel/docker-compose.yml up -d
```

- 主节点：`127.0.0.1:6380`；从节点：6381 / 6382；哨兵：26380 / 26381 / 26382。
- master name 固定为 `binomic`，quorum 为 2。

健康判定：

```bash
redis-cli -p 6380 ping                                            # PONG
redis-cli -p 6381 info replication | grep master_link_status      # up
redis-cli -p 26380 sentinel get-master-addr-by-name binomic       # 127.0.0.1 6380
```

销毁：

```bash
docker compose -f assets/sentinel/docker-compose.yml down
```

## Cluster 拓扑（3 主 3 从）

```bash
docker compose -f assets/cluster/docker-compose.yml up -d
```

- 节点：`127.0.0.1:7000` … `7005`；cluster bus 端口 17000 … 17005（仅本机）。
- `cluster-init` 一次性服务负责等待 6 节点就绪并执行
  `redis-cli --cluster create --cluster-replicas 1`。

健康判定（`cluster-init` 退出码为 0，且）：

```bash
docker compose -f assets/cluster/docker-compose.yml ps   # cluster-init exited (0)
redis-cli -p 7000 cluster info | grep cluster_state       # ok
redis-cli -p 7000 cluster info | grep cluster_slots_assigned  # 16384
```

销毁：

```bash
docker compose -f assets/cluster/docker-compose.yml down
```

`down` 会连同容器内 `/tmp/nodes.conf` 一起丢弃（节点配置写在容器文件系统，
不使用数据卷），因此重复 `up` 不会有残留状态。

## 与测试套件的联动

```bash
uv run pytest test_integration -m ha
```

- 哨兵入口端口可用 `BINOMIC_HA_SENTINEL_PORT` 覆盖（默认 26380，
  其余两个哨兵按 +1/+2 派生）；Cluster 启动节点可用
  `BINOMIC_HA_CLUSTER_NODE` 覆盖（默认 `127.0.0.1:7000`）。
- 环境未启动时对应套件整体 skip，不视为失败。
