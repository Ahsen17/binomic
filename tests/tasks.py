from binomic.task.registry import TaskRegistry, TaskSpec, registry


class TestTasksModule:
    def test_import_registers_example_task(self, isolate_registry: TaskRegistry) -> None:

        import binomic.tasks  # noqa: F401, PLC0415 - import-time registration is the behavior

        spec = registry.get("example")

        assert isinstance(spec, TaskSpec)
        assert spec.mode == "direct"
