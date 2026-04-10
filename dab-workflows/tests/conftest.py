"""Pytest fixtures for notebook testing."""

import tempfile
from pathlib import Path

import pytest
import yaml


@pytest.fixture
def sample_config_data():
    """Sample configuration data for testing."""
    return {
        "TRANSLATION_MODEL": "databricks-meta-llama-3-3-70b-instruct",
        "CLASSIFICATION_MODEL": "databricks-claude-sonnet-4-5",
        "LLM_MODEL": "databricks-meta-llama-3-3-70b-instruct",
        "TRANSLATE_PROMPT": "translate to english. Retain the original formatting.",
        "SEGMENT_PROMPT": "Analyze this document and identify CVs.",
        "PROMPT": "Extract structured data from the document.",
        "RESPONSE_FORMAT": "json",
        "AGENT_BRICKS_ENDPOINT": "test-endpoint",
    }


@pytest.fixture
def temp_config_file(sample_config_data, tmp_path):
    """Create a temporary config YAML file."""
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(sample_config_data, f)
    return str(config_path)


@pytest.fixture
def empty_config_file(tmp_path):
    """Create an empty config YAML file."""
    config_path = tmp_path / "empty_config.yaml"
    with open(config_path, "w") as f:
        f.write("")
    return str(config_path)


@pytest.fixture
def sample_widget_params():
    """Sample widget parameters for testing."""
    return {
        "catalog": "fins_genai",
        "schema": "genai_hackathon",
        "volume": "docs_for_redaction",
        "table_prefix": "redaction_workflow",
        "partition_count": "5",
        "checkpoint_location": "/Volumes/fins_genai/genai_hackathon/checkpoints/workflow/step1",
    }


@pytest.fixture
def sample_file_paths():
    """Sample file paths for testing."""
    return [
        "/Volumes/catalog/schema/volume/document.pdf",
        "/Volumes/catalog/schema/volume/presentation.pptx",
        "/Volumes/catalog/schema/volume/document.docx",
        "/Volumes/catalog/schema/volume/image.png",
        "/Volumes/catalog/schema/volume/image.jpg",
        "/Volumes/catalog/schema/volume/no_extension",
        "/Volumes/catalog/schema/volume/file.txt",
    ]


@pytest.fixture
def mock_dbutils():
    """Mock dbutils for testing notebook code."""

    class MockWidgets:
        def __init__(self):
            self._widgets = {}

        def text(self, name, default_value, label):
            self._widgets[name] = default_value

        def dropdown(self, name, choices, defaultValue, label):
            self._widgets[name] = defaultValue

        def get(self, name):
            return self._widgets.get(name, "")

    class MockDbutils:
        def __init__(self):
            self.widgets = MockWidgets()

    return MockDbutils()


@pytest.fixture
def sample_json_responses():
    """Sample JSON responses with various formats."""
    return {
        "clean": '[{"item_number": 1, "type": "CV"}]',
        "with_markdown": '```json\n[{"item_number": 1, "type": "CV"}]\n```',
        "empty_array": "[]",
        "nested": '{"results": [{"id": 1}]}',
    }
