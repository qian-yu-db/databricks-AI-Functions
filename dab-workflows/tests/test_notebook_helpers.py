"""Unit tests for notebook_helpers module."""

import pytest
import yaml

from src.notebook_helpers import (
    NotebookConfig,
    VolumePathConfig,
    TableNameConfig,
    get_source_path,
    get_image_path,
    get_checkpoint_location,
    derive_table_name,
    validate_widget_params,
    parse_partition_count,
    extract_file_extension,
    is_supported_document_type,
    build_ai_parse_options,
    clean_json_response,
    validate_config_keys,
)


class TestNotebookConfig:
    """Tests for NotebookConfig class."""

    def test_from_yaml_loads_config(self, temp_config_file, sample_config_data):
        """Test that from_yaml correctly loads a YAML config file."""
        config = NotebookConfig.from_yaml(temp_config_file)

        assert config.config_path == temp_config_file
        assert config.data == sample_config_data

    def test_from_yaml_missing_file_raises_error(self):
        """Test that from_yaml raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            NotebookConfig.from_yaml("/nonexistent/config.yaml")

    def test_from_yaml_empty_file(self, empty_config_file):
        """Test that from_yaml handles empty config file."""
        config = NotebookConfig.from_yaml(empty_config_file)
        assert config.data == {}

    def test_get_returns_value(self, temp_config_file):
        """Test that get() returns the correct value."""
        config = NotebookConfig.from_yaml(temp_config_file)
        assert config.get("LLM_MODEL") == "databricks-meta-llama-3-3-70b-instruct"

    def test_get_returns_default_for_missing_key(self, temp_config_file):
        """Test that get() returns default for missing key."""
        config = NotebookConfig.from_yaml(temp_config_file)
        assert config.get("NONEXISTENT_KEY", "default") == "default"

    def test_getitem_returns_value(self, temp_config_file):
        """Test that __getitem__ returns the correct value."""
        config = NotebookConfig.from_yaml(temp_config_file)
        assert config["LLM_MODEL"] == "databricks-meta-llama-3-3-70b-instruct"

    def test_getitem_raises_keyerror_for_missing_key(self, temp_config_file):
        """Test that __getitem__ raises KeyError for missing key."""
        config = NotebookConfig.from_yaml(temp_config_file)
        with pytest.raises(KeyError):
            _ = config["NONEXISTENT_KEY"]


class TestVolumePathConfig:
    """Tests for VolumePathConfig class."""

    def test_base_path(self):
        """Test that base_path generates correct path."""
        config = VolumePathConfig(
            catalog="fins_genai",
            schema="genai_hackathon",
            volume="docs"
        )
        assert config.base_path == "/Volumes/fins_genai/genai_hackathon/docs"

    def test_get_subpath_single(self):
        """Test get_subpath with single subdirectory."""
        config = VolumePathConfig(
            catalog="fins_genai",
            schema="genai_hackathon",
            volume="docs"
        )
        path = config.get_subpath("images")
        assert path == "/Volumes/fins_genai/genai_hackathon/docs/images"

    def test_get_subpath_multiple(self):
        """Test get_subpath with multiple subdirectories."""
        config = VolumePathConfig(
            catalog="fins_genai",
            schema="genai_hackathon",
            volume="docs"
        )
        path = config.get_subpath("outputs", "extracted", "images")
        assert path == "/Volumes/fins_genai/genai_hackathon/docs/outputs/extracted/images"


class TestTableNameConfig:
    """Tests for TableNameConfig class."""

    def test_get_table_name_with_prefix(self):
        """Test get_table_name with prefix."""
        config = TableNameConfig(
            catalog="fins_genai",
            schema="genai_hackathon",
            table_prefix="redaction_workflow"
        )
        assert config.get_table_name("raw_parsed_files") == "redaction_workflow_raw_parsed_files"

    def test_get_table_name_without_prefix(self):
        """Test get_table_name without prefix."""
        config = TableNameConfig(
            catalog="fins_genai",
            schema="genai_hackathon",
            table_prefix=""
        )
        assert config.get_table_name("raw_parsed_files") == "raw_parsed_files"

    def test_get_full_table_name(self):
        """Test get_full_table_name generates fully qualified name."""
        config = TableNameConfig(
            catalog="fins_genai",
            schema="genai_hackathon",
            table_prefix="workflow"
        )
        result = config.get_full_table_name("parsed_docs")
        assert result == "fins_genai.genai_hackathon.workflow_parsed_docs"


class TestPathFunctions:
    """Tests for path generation functions."""

    def test_get_source_path(self):
        """Test get_source_path generates correct path."""
        path = get_source_path("catalog", "schema", "volume")
        assert path == "/Volumes/catalog/schema/volume/original"

    def test_get_source_path_custom_subpath(self):
        """Test get_source_path with custom subpath."""
        path = get_source_path("catalog", "schema", "volume", "custom")
        assert path == "/Volumes/catalog/schema/volume/custom"

    def test_get_image_path(self):
        """Test get_image_path generates correct path."""
        path = get_image_path("catalog", "schema", "volume")
        assert path == "/Volumes/catalog/schema/volume/images"

    def test_get_image_path_custom_subpath(self):
        """Test get_image_path with custom subpath."""
        path = get_image_path("catalog", "schema", "volume", "extracted")
        assert path == "/Volumes/catalog/schema/volume/extracted"

    def test_get_checkpoint_location(self):
        """Test get_checkpoint_location generates correct path."""
        path = get_checkpoint_location(
            "fins_genai", "hackathon", "parse_workflow", "01_parse"
        )
        assert path == "/Volumes/fins_genai/hackathon/checkpoints/parse_workflow/01_parse"


class TestTableNameFunctions:
    """Tests for table name functions."""

    def test_derive_table_name_with_prefix(self):
        """Test derive_table_name with prefix."""
        name = derive_table_name("workflow", "parsed_files")
        assert name == "workflow_parsed_files"

    def test_derive_table_name_without_prefix(self):
        """Test derive_table_name without prefix."""
        name = derive_table_name("", "parsed_files")
        assert name == "parsed_files"


class TestValidateWidgetParams:
    """Tests for validate_widget_params function."""

    def test_all_params_present(self, sample_widget_params):
        """Test validation passes when all params present."""
        required = ["catalog", "schema", "volume"]
        missing = validate_widget_params(sample_widget_params, required)
        assert missing == []

    def test_missing_params(self, sample_widget_params):
        """Test validation detects missing params."""
        required = ["catalog", "schema", "missing_param"]
        missing = validate_widget_params(sample_widget_params, required)
        assert "missing_param" in missing

    def test_empty_param_value(self):
        """Test validation detects empty param values."""
        params = {"catalog": "value", "schema": ""}
        required = ["catalog", "schema"]
        missing = validate_widget_params(params, required)
        assert "schema" in missing


class TestParsePartitionCount:
    """Tests for parse_partition_count function."""

    def test_valid_value(self):
        """Test parsing valid partition count."""
        assert parse_partition_count("10") == 10

    def test_invalid_value_returns_default(self):
        """Test invalid value returns default."""
        assert parse_partition_count("invalid") == 5
        assert parse_partition_count("invalid", default=8) == 8

    def test_zero_raises_error(self):
        """Test zero value raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            parse_partition_count("0")

    def test_negative_raises_error(self):
        """Test negative value raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            parse_partition_count("-5")

    def test_exceeds_max_raises_error(self):
        """Test value exceeding max raises ValueError."""
        with pytest.raises(ValueError, match="exceeds maximum"):
            parse_partition_count("150", max_value=100)

    @pytest.mark.parametrize("value,expected", [
        ("1", 1),
        ("5", 5),
        ("50", 50),
        ("100", 100),
    ])
    def test_valid_values(self, value, expected):
        """Test various valid partition count values."""
        assert parse_partition_count(value) == expected


class TestExtractFileExtension:
    """Tests for extract_file_extension function."""

    @pytest.mark.parametrize("path,expected", [
        ("/path/to/file.pdf", "pdf"),
        ("/path/to/file.PDF", "pdf"),
        ("/path/to/file.pptx", "pptx"),
        ("/path/to/file.DOCX", "docx"),
        ("/path/to/file.tar.gz", "gz"),
        ("/path/to/file", ""),
        ("file.txt", "txt"),
    ])
    def test_extract_extension(self, path, expected):
        """Test file extension extraction."""
        assert extract_file_extension(path) == expected


class TestIsSupportedDocumentType:
    """Tests for is_supported_document_type function."""

    @pytest.mark.parametrize("path,expected", [
        ("/path/to/file.pdf", True),
        ("/path/to/file.pptx", True),
        ("/path/to/file.docx", True),
        ("/path/to/file.PDF", True),
        ("/path/to/file.txt", False),
        ("/path/to/file.png", False),
        ("/path/to/file", False),
    ])
    def test_default_supported_types(self, path, expected):
        """Test default supported document types."""
        assert is_supported_document_type(path) == expected

    def test_custom_supported_types(self):
        """Test custom supported types."""
        assert is_supported_document_type("/path/file.png", ["png", "jpg"]) is True
        assert is_supported_document_type("/path/file.pdf", ["png", "jpg"]) is False


class TestBuildAiParseOptions:
    """Tests for build_ai_parse_options function."""

    def test_default_options(self):
        """Test default options."""
        options = build_ai_parse_options()
        assert options["version"] == "2.0"
        assert options["descriptionElementTypes"] == "*"
        assert "imageOutputPath" not in options

    def test_with_image_output_path(self):
        """Test options with image output path."""
        options = build_ai_parse_options(image_output_path="/path/to/images")
        assert options["imageOutputPath"] == "/path/to/images"

    def test_custom_version(self):
        """Test custom version."""
        options = build_ai_parse_options(version="1.0")
        assert options["version"] == "1.0"

    def test_custom_description_types(self):
        """Test custom description element types."""
        options = build_ai_parse_options(description_element_types="tables,images")
        assert options["descriptionElementTypes"] == "tables,images"


class TestCleanJsonResponse:
    """Tests for clean_json_response function."""

    def test_clean_response_unchanged(self, sample_json_responses):
        """Test clean response remains unchanged."""
        result = clean_json_response(sample_json_responses["clean"])
        assert result == '[{"item_number": 1, "type": "CV"}]'

    def test_removes_markdown_blocks(self, sample_json_responses):
        """Test markdown code blocks are removed."""
        result = clean_json_response(sample_json_responses["with_markdown"])
        assert "```" not in result
        assert result == '[{"item_number": 1, "type": "CV"}]'

    def test_handles_empty_array(self, sample_json_responses):
        """Test empty array is handled."""
        result = clean_json_response(sample_json_responses["empty_array"])
        assert result == "[]"


class TestValidateConfigKeys:
    """Tests for validate_config_keys function."""

    def test_all_keys_present(self, sample_config_data):
        """Test validation passes when all keys present."""
        required = ["LLM_MODEL", "TRANSLATE_PROMPT"]
        missing = validate_config_keys(sample_config_data, required)
        assert missing == []

    def test_missing_keys(self, sample_config_data):
        """Test validation detects missing keys."""
        required = ["LLM_MODEL", "MISSING_KEY", "ANOTHER_MISSING"]
        missing = validate_config_keys(sample_config_data, required)
        assert "MISSING_KEY" in missing
        assert "ANOTHER_MISSING" in missing
        assert "LLM_MODEL" not in missing

    def test_empty_config(self):
        """Test validation with empty config."""
        missing = validate_config_keys({}, ["key1", "key2"])
        assert missing == ["key1", "key2"]
