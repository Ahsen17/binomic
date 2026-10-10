# Litestar integration

Binomic ships a {class}`~binomic.plugin.litestar.BinomicPlugin` that injects
the `Binomic` client into [Litestar](https://docs.litestar.dev/) dependencies:

```python
from binomic.config import BinomicConfig
from binomic.plugin.litestar import BinomicPlugin
from litestar import Litestar

app = Litestar(
    plugins=[
        BinomicPlugin(
            app_name="myapp",
            broker_dsn="redis://localhost:6379/0",
            config=BinomicConfig(queues=["default"]),
        )
    ],
)
```

Inside handlers, the `Binomic` client is injected through the `binomic`
parameter.
