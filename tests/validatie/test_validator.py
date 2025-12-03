"""Unit tests for KRM validator components."""

import numpy as np
import pandas as pd
import pytest

from krm_validator.config import ValidationConfig
from krm_validator.report import ValidationReport, ValidationResult, ValidationSection
from krm_validator.validator import KRMValidator


class TestValidationConfig:
    """Tests for ValidationConfig."""
    
    def test_default_values(self):
        config = ValidationConfig()
        assert config.bucket_name == "krm-validatie-data-prod"
        assert config.is_local is True
        assert config.max_location_distance_m == 100.0
    
    def test_temp_folder_local(self):
        config = ValidationConfig(is_local=True)
        assert config.temp_folder == config.local_folder
    
    def test_temp_folder_lambda(self, tmp_path):
        config = ValidationConfig(is_local=False, local_folder=tmp_path)
        from pathlib import Path
        assert config.temp_folder == Path("/tmp")


class TestValidationReport:
    """Tests for ValidationReport."""
    
    def test_empty_report_is_valid(self):
        report = ValidationReport()
        assert report.is_valid is True
        assert report.failure_count == 0
    
    def test_add_failure(self):
        report = ValidationReport()
        report.add(
            section=ValidationSection.GEO_CONTROL,
            databundelcode="test_bundle",
            record_id="NL80_123",
            uitvalreden="test error",
            informatie="test info"
        )
        
        assert report.is_valid is False
        assert report.failure_count == 1
        assert report.results[0].record_id == "123"  # NL80_ stripped
    
    def test_clean_record_id(self):
        assert ValidationReport._clean_record_id("NL80_ABC") == "ABC"
        assert ValidationReport._clean_record_id("XYZ") == "XYZ"
        assert ValidationReport._clean_record_id(123) == "123"
    
    def test_failures_by_section(self):
        report = ValidationReport()
        report.add(ValidationSection.GEO_CONTROL, "b", "1", "e", "i")
        report.add(ValidationSection.GEO_CONTROL, "b", "2", "e", "i")
        report.add(ValidationSection.DATE_RANGE, "b", "3", "e", "i")
        
        by_section = report.failures_by_section()
        assert by_section[ValidationSection.GEO_CONTROL] == 2
        assert by_section[ValidationSection.DATE_RANGE] == 1
    
    def test_to_dataframe(self):
        report = ValidationReport()
        report.add(ValidationSection.GEO_CONTROL, "bundle", "rec1", "error", "info")
        
        df = report.to_dataframe()
        assert len(df) == 1
        assert df.iloc[0]['section'] == "Geo controle"
        assert df.iloc[0]['record_id'] == "rec1"
    
    def test_to_csv(self, tmp_path):
        report = ValidationReport()
        report.add(ValidationSection.COLUMN_CHECK, "b", "r", "e", "i")
        
        filepath = tmp_path / "report.csv"
        report.to_csv(filepath)
        
        assert filepath.exists()
        df = pd.read_csv(filepath)
        assert len(df) == 1
        assert "Section" in df.columns


class TestKRMValidatorHelpers:
    """Tests for KRMValidator helper methods."""
    
    def test_values_match_both_present(self):
        assert KRMValidator._values_match("A", "A") is True
        assert KRMValidator._values_match("A", "B") is False
    
    def test_values_match_both_nan(self):
        assert KRMValidator._values_match(np.nan, np.nan) is True
        assert KRMValidator._values_match(pd.NA, pd.NA) is True
    
    def test_values_match_one_nan(self):
        assert KRMValidator._values_match("A", np.nan) is False
        assert KRMValidator._values_match(np.nan, "A") is False
    
    def test_value_in_list_simple(self):
        assert KRMValidator._value_in_list("A", "A;B;C") is True
        assert KRMValidator._value_in_list("D", "A;B;C") is False
    
    def test_value_in_list_nan(self):
        assert KRMValidator._value_in_list(np.nan, np.nan) is True
        assert KRMValidator._value_in_list("A", np.nan) is False
    
    def test_get_parameter_value_biotaxon(self):
        row = pd.Series({
            'biotaxon.naam': 'Species Name',
            'parameter.code': np.nan
        })
        result = KRMValidator._get_parameter_value(row)
        assert result == 'species name'
    
    def test_get_parameter_value_parameter_code(self):
        row = pd.Series({
            'biotaxon.naam': np.nan,
            'parameter.code': 'PARAM1'
        })
        result = KRMValidator._get_parameter_value(row)
        assert result == 'param1'
    
    def test_get_parameter_value_neither(self):
        row = pd.Series({
            'biotaxon.naam': np.nan,
            'parameter.code': np.nan
        })
        result = KRMValidator._get_parameter_value(row)
        assert pd.isna(result)


class TestValidationSection:
    """Tests for ValidationSection enum."""
    
    def test_all_sections_have_dutch_names(self):
        for section in ValidationSection:
            assert section.value  # Not empty
            # All section names should be in Dutch
            assert any(word in section.value.lower() for word in 
                      ['controle', 'check'])


# Integration test placeholder
class TestIntegration:
    """Integration tests (require reference data)."""
    
    @pytest.mark.skip(reason="Requires GitHub access for reference data")
    def test_full_validation_pipeline(self):
        """Test complete validation flow."""
        pass
