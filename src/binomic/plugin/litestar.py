from typing import TYPE_CHECKING, ClassVar, cast

from binomic.client import Binomic, BinomicFactory

if TYPE_CHECKING:
    from binomic.config import BinomicConfig

try:
    import litestar  # type: ignore # noqa: F401
    from litestar.di import Provide  # type: ignore
    from litestar.plugins import InitPluginProtocol  # type: ignore

    if TYPE_CHECKING:
        from litestar.config.app import AppConfig  # type: ignore
        from litestar.data_structures import State  # type: ignore

except ImportError:
    raise ImportError(  # noqa: B904
        "litestar is not installed. Please install it using pip install litestar."
    )


__all__ = ("BinomicPlugin",)


class BinomicPlugin(InitPluginProtocol):  # type: ignore
    """Binomic plugin for litestar."""

    _binomic_factory_state_key: ClassVar[str] = "binomic_factory"

    def __init__(
        self,
        app_name: str,
        broker_dsn: str,
        redis_dsn: str,
        config: "BinomicConfig",
    ) -> None:

        self._app_name = app_name
        self._broker_dsn = broker_dsn
        self._redis_dsn = redis_dsn
        self._config = config

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":

        self.setup_signature_namespaces(app_config)
        self.setup_states(app_config)
        self.setup_dependencies(app_config)

        return app_config

    def provide_binomic(self, state: "State") -> "Binomic":

        if (
            factory := cast(
                "BinomicFactory | None",
                state.get(self._binomic_factory_state_key),
            )
        ) is None:
            raise RuntimeError(
                "BinomicFactory is not provided. "
                "Please ensure that the BinomicPlugin is properly configured."
            )

        return factory.create()

    def setup_signature_namespaces(self, app_config: "AppConfig") -> None:

        app_config.signature_namespace.update(
            {
                "Binomic": Binomic,
                "BinomicFactory": BinomicFactory,
            }
        )

    def setup_states(self, app_config: "AppConfig") -> None:

        need_setup = {}

        state = app_config.state
        if self._binomic_factory_state_key not in state:
            need_setup[self._binomic_factory_state_key] = BinomicFactory(
                broker_dsn=self._broker_dsn,
                redis_dsn=self._redis_dsn,
                module_name=self._app_name,
                config=self._config,
            )

        state.update(need_setup)

    def setup_dependencies(self, app_config: "AppConfig") -> None:

        app_config.dependencies.update(
            {
                "binomic": Provide(
                    dependency=self.provide_binomic,
                    sync_to_thread=True,
                )
            }
        )
