"""CDR Silver Pipeline modules."""

try:
    from pipeline import PipelineResult, run_pipeline
except ModuleNotFoundError:
    from .pipeline import PipelineResult, run_pipeline

__all__ = ["PipelineResult", "run_pipeline"]
