"""
IPipelineRepository port.
Abstracts persistence of canvas/pipeline data.
"""
from abc import ABC, abstractmethod
from typing import Any


class IPipelineRepository(ABC):

    @abstractmethod
    def load_canvas(self, canvas_id: int) -> dict[str, Any]:
        """Return {'nodes': [...], 'edges': [...]} for the canvas."""

    @abstractmethod
    def save_canvas(self, canvas_id: int, nodes: list[dict], edges: list[dict]) -> None:
        """Persist the full canvas state (nodes + edges)."""

    @abstractmethod
    def load_node_config(self, node_id: str) -> dict[str, Any]:
        """Return the config dict for a single node."""

    @abstractmethod
    def save_node_output_metadata(self, node_id: str, metadata: dict) -> None:
        """Persist the output column metadata for a node."""
