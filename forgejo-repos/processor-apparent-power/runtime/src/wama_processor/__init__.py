"""Small, domain-oriented building blocks for WAMA measurement processors."""

from .definition import (
    DerivedMeasurement,
    InputMeasurement,
    ProcessorDefinition,
    ProcessorDefinitionError,
)
from .runtime import (
    RuntimeConfig,
    RuntimeConfigurationError,
    build_application,
    build_output_stream,
    build_transformation_stream,
    run_processor,
)

__all__ = [
    "DerivedMeasurement",
    "InputMeasurement",
    "ProcessorDefinition",
    "ProcessorDefinitionError",
    "RuntimeConfig",
    "RuntimeConfigurationError",
    "build_application",
    "build_output_stream",
    "build_transformation_stream",
    "run_processor",
]
