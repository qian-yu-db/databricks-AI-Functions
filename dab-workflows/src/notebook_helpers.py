"""
Shared helper functions for Databricks notebook asset bundles.

This module extracts testable logic from notebooks to enable unit testing
without requiring Spark or Databricks runtime.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class NotebookConfig:
    """Configuration loaded from a YAML file."""

    config_path: str
    data: dict[str, Any]

    @classmethod
    def from_yaml(cls, config_path: str) -> "NotebookConfig":
        """Load configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file.

        Returns:
            NotebookConfig instance with loaded data.

        Raises:
            FileNotFoundError: If config file doesn't exist.
            yaml.YAMLError: If config file is invalid YAML.
        """
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)
        return cls(config_path=config_path, data=data or {})

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by key."""
        return self.data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        """Get a configuration value by key (dict-like access)."""
        return self.data[key]


@dataclass
class VolumePathConfig:
    """Configuration for Databricks volume paths."""

    catalog: str
    schema: str
    volume: str

    @property
    def base_path(self) -> str:
        """Get the base volume path."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    def get_subpath(self, *subpaths: str) -> str:
        """Get a path within the volume.

        Args:
            *subpaths: Subdirectory names to append.

        Returns:
            Full path string.
        """
        path = Path(self.base_path)
        for subpath in subpaths:
            path = path / subpath
        return str(path)


@dataclass
class TableNameConfig:
    """Configuration for Databricks table names."""

    catalog: str
    schema: str
    table_prefix: str = ""

    def get_table_name(self, suffix: str) -> str:
        """Get a table name with the given suffix.

        Args:
            suffix: Table name suffix (e.g., 'raw_parsed_files').

        Returns:
            Table name with prefix applied.
        """
        if self.table_prefix:
            return f"{self.table_prefix}_{suffix}"
        return suffix

    def get_full_table_name(self, suffix: str) -> str:
        """Get the fully qualified table name.

        Args:
            suffix: Table name suffix.

        Returns:
            Fully qualified table name (catalog.schema.table).
        """
        table_name = self.get_table_name(suffix)
        return f"{self.catalog}.{self.schema}.{table_name}"


def get_source_path(catalog: str, schema: str, volume: str, subpath: str = "original") -> str:
    """Generate source path for reading files.

    Args:
        catalog: Databricks catalog name.
        schema: Databricks schema name.
        volume: Volume name.
        subpath: Subdirectory within volume (default: 'original').

    Returns:
        Full source path string.
    """
    return f"/Volumes/{catalog}/{schema}/{volume}/{subpath}"


def get_image_path(catalog: str, schema: str, volume: str, subpath: str = "images") -> str:
    """Generate image output path.

    Args:
        catalog: Databricks catalog name.
        schema: Databricks schema name.
        volume: Volume name.
        subpath: Subdirectory for images (default: 'images').

    Returns:
        Full image path string.
    """
    return f"/Volumes/{catalog}/{schema}/{volume}/{subpath}"


def get_checkpoint_location(
    catalog: str, schema: str, workflow_name: str, step_name: str
) -> str:
    """Generate checkpoint location path.

    Args:
        catalog: Databricks catalog name.
        schema: Databricks schema name.
        workflow_name: Name of the workflow.
        step_name: Name of the processing step.

    Returns:
        Checkpoint location path.
    """
    return f"/Volumes/{catalog}/{schema}/checkpoints/{workflow_name}/{step_name}"


def derive_table_name(table_prefix: str, suffix: str) -> str:
    """Derive table name from prefix and suffix.

    Args:
        table_prefix: Prefix for table names.
        suffix: Suffix to append.

    Returns:
        Combined table name.
    """
    if table_prefix:
        return f"{table_prefix}_{suffix}"
    return suffix


def validate_widget_params(params: dict[str, str], required_keys: list[str]) -> list[str]:
    """Validate that all required widget parameters are present.

    Args:
        params: Dictionary of parameter name to value.
        required_keys: List of required parameter names.

    Returns:
        List of missing parameter names (empty if all present).
    """
    missing = []
    for key in required_keys:
        if key not in params or not params[key]:
            missing.append(key)
    return missing


def parse_partition_count(value: str, default: int = 5, max_value: int = 100) -> int:
    """Parse and validate partition count from string.

    Args:
        value: String value to parse.
        default: Default value if parsing fails.
        max_value: Maximum allowed value.

    Returns:
        Validated partition count.

    Raises:
        ValueError: If value is negative or exceeds max_value.
    """
    try:
        count = int(value)
    except (ValueError, TypeError):
        return default

    if count <= 0:
        raise ValueError(f"Partition count must be positive, got {count}")
    if count > max_value:
        raise ValueError(f"Partition count {count} exceeds maximum {max_value}")

    return count


def extract_file_extension(path: str) -> str:
    """Extract file extension from a path.

    Args:
        path: File path string.

    Returns:
        File extension without the dot, or empty string if none.
    """
    if "." not in path:
        return ""
    return path.rsplit(".", 1)[-1].lower()


def is_supported_document_type(path: str, supported_types: list[str] | None = None) -> bool:
    """Check if a file path has a supported document type.

    Args:
        path: File path to check.
        supported_types: List of supported extensions (default: pdf, pptx, docx).

    Returns:
        True if file type is supported.
    """
    if supported_types is None:
        supported_types = ["pdf", "pptx", "docx"]

    ext = extract_file_extension(path)
    return ext in supported_types


def build_ai_parse_options(
    version: str = "2.0",
    image_output_path: str | None = None,
    description_element_types: str = "*",
) -> dict[str, str]:
    """Build options dictionary for ai_parse_document.

    Args:
        version: API version to use.
        image_output_path: Path for extracted images.
        description_element_types: Element types to describe.

    Returns:
        Options dictionary for ai_parse_document.
    """
    options = {
        "version": version,
        "descriptionElementTypes": description_element_types,
    }
    if image_output_path:
        options["imageOutputPath"] = image_output_path
    return options


def clean_json_response(response: str) -> str:
    """Clean markdown code blocks from JSON response.

    Args:
        response: Raw response that may contain ```json blocks.

    Returns:
        Cleaned JSON string.
    """
    # Remove ```json and ``` markers
    cleaned = response.replace("```json", "").replace("```", "")
    return cleaned.strip()


def validate_config_keys(config: dict[str, Any], required_keys: list[str]) -> list[str]:
    """Validate that required keys exist in config.

    Args:
        config: Configuration dictionary.
        required_keys: List of required key names.

    Returns:
        List of missing keys.
    """
    return [key for key in required_keys if key not in config]
