"""Errors the pipeline raises on purpose. Anything else is a bug and keeps its own traceback."""


class PipelineError(RuntimeError):
    """A stage cannot run: missing input, wrong runtime, unreachable serving store."""
