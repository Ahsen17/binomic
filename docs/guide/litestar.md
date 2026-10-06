# Litestar 集成

Binomic 内置 {class}`~binomic.plugin.litestar.BinomicPlugin`，将 `Binomic`
客户端注入 [Litestar](https://docs.litestar.dev/) 依赖体系：

```python
from binomic.config import BinomicConfig
from binomic.plugin.litestar import BinomicPlugin
from litestar import Litestar

app = Litestar(
    plugins=[
        BinomicPlugin(
            app_name="myapp",
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/1",
            config=BinomicConfig(queues=["default"]),
        )
    ],
)
```

处理器内通过 `binomic` 参数注入 `Binomic` 客户端。
