from binomic.base.constants import APP_NAME


class TestConstants:
    def test_app_name_is_binomic(self) -> None:

        assert APP_NAME == "binomic"
