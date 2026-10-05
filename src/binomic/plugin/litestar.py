from typing import TYPE_CHECKING, Literal

from binomic.task.wrappers import autodiscover

try:
    import litestar  # type: ignore # noqa: F401
    from litestar.plugins import InitPluginProtocol  # type: ignore

    if TYPE_CHECKING:
        from litestar.config.app import AppConfig  # type: ignore

except ImportError:
    raise ImportError(  # noqa: B904
        "litestar is not installed. Please install it using pip install litestar."
    )


__all__ = ("BinomicPlugin",)


class BinomicPlugin(InitPluginProtocol):  # type: ignore
    """Binomic plugin for litestar."""

    def __init__(
        self,
        app_name: str,
        *,
        on_error: Literal["warn", "raise"] = "warn",
    ) -> None:

        self._app_name = app_name
        autodiscover(app_name, on_error=on_error)

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":

        return app_config
