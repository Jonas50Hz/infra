"""Validated authoring primitives for WAMA processor repositories."""

from wama_processor_authoring.catalog import (
    ApprovalCatalog,
    InputCatalog,
    ResolvedProcessor,
    load_approval_catalog,
    load_input_catalog,
    resolve_processor,
)
from wama_processor_authoring.manifest import ProcessorManifest, load_manifest
from wama_processor_authoring.simulation import (
    InputSample,
    LatestValuesSimulator,
    simulate_formula,
)

__all__ = [
    "ApprovalCatalog",
    "InputCatalog",
    "InputSample",
    "LatestValuesSimulator",
    "ProcessorManifest",
    "ResolvedProcessor",
    "load_approval_catalog",
    "load_input_catalog",
    "load_manifest",
    "resolve_processor",
    "simulate_formula",
]