from datetime import date
import os
import pytest
import sys
import requests
import polars as pl
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from lib.workspace.github_client import GitHubRelease
from lib.extract.ejari_rents_downloader import EjariRentsDownloader
from lib.transform.rents_transformer import RentsTransformer
from lib.classes.property_usage import PropertyUsage
from lib.classes.validators import RentContractValidator, validate_rent_contracts

import pytest
import requests_mock

class TestGitHubRelease:
    @classmethod
    def setup_class(cls):
        """Setup resources before any tests run."""
        cls.repo = "test_owner/test_repo"
        cls.mock_release = {
            "upload_url": "https://api.github.com/repos/test_owner/test_repo/releases/1/assets{?name,label}",
            "name": "Test Release"
        }
    
    def setup_method(self):
        """Setup for each test method."""
        with patch.dict(os.environ, {'GH_TOKEN': 'test_token'}):
            self.github_release = GitHubRelease(self.repo)
    
    def test_create_release(self, requests_mock):
        """Test creating a new GitHub release."""
        requests_mock.post(f"https://api.github.com/repos/{self.repo}/releases", json=self.mock_release, status_code=201)
        release = self.github_release.create_release()
        assert release == self.mock_release
    
    def test_upload_files(self, requests_mock, tmp_path):
        """Test uploading files to a GitHub release."""
        file_path = tmp_path / "test_file.txt"
        file_path.write_text("Test content")
        requests_mock.post(self.mock_release["upload_url"].split("{")[0] + "?name=test_file.txt", status_code=201)
        self.github_release.upload_files(self.mock_release, [str(file_path)])
    
    def test_release_exists(self, requests_mock):
        """Test checking if a release exists."""
        tag_name = "release-2025-02-28"
        requests_mock.get(f"https://api.github.com/repos/{self.repo}/releases/tags/{tag_name}", status_code=200)
        assert self.github_release.release_exists(tag_name) is True

        requests_mock.get(f"https://api.github.com/repos/{self.repo}/releases/tags/{tag_name}", status_code=404)
        assert self.github_release.release_exists(tag_name) is False
    
    def test_publish(self, requests_mock, tmp_path):
        """Test publishing files to a new release."""
        file_path = tmp_path / "test_file.txt"
        file_path.write_text("Test content")
        
        requests_mock.post(f"https://api.github.com/repos/{self.repo}/releases", json=self.mock_release, status_code=201)
        requests_mock.post(self.mock_release["upload_url"].split("{")[0] + "?name=test_file.txt", status_code=201)
        
        self.github_release.publish([str(file_path)])
    
    def test_init_without_token(self):
        """Test initialization fails without GH_TOKEN."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="GH_TOKEN not set"):
                GitHubRelease(self.repo)


class TestEjariRentsDownloader:
    def setup_method(self):
        self.test_url = "https://example.com/rents"
        self.downloader = EjariRentsDownloader(self.test_url)

    def test_body(self):
        body = self.downloader._body(1000, 0, from_date="09/12/2026", to_date="09/13/2026")
        assert body["P_FROM_DATE"] == "09/12/2026"
        assert body["P_TAKE"] == "1000"
        assert body["P_SKIP"] == "0"
        assert body["P_DATE_TYPE"] == "0"

    @patch('lib.extract.ejari_rents_downloader.requests.post')
    def test_run_success(self, mock_post, tmp_path):
        mock_resp = Mock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "responseCode": 200,
            "response": {"result": [
                {"RN": 1, "TOTAL": 2, "ANNUAL_AMOUNT": 50000, "USAGE_EN": "Residential"},
                {"RN": 2, "TOTAL": 2, "ANNUAL_AMOUNT": 70000, "USAGE_EN": "Commercial"},
            ]},
        }
        mock_post.return_value = mock_resp
        out = tmp_path / "rents.csv"
        assert self.downloader.run(str(out), from_date="09/12/2026", to_date="09/13/2026") is True
        assert out.exists()

    @patch('lib.extract.ejari_rents_downloader.requests.post')
    def test_run_gateway_error(self, mock_post, tmp_path):
        mock_resp = Mock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"responseCode": 420, "validationErrorsList": [{"errorMessage": "INVALID_REQUEST"}]}
        mock_post.return_value = mock_resp
        out = tmp_path / "rents.csv"
        with patch('time.sleep'):
            assert self.downloader.run(str(out)) is False


class TestRentsTransformer:
    def setup_method(self):
        self.input_file = "test_input.csv"
        self.output_file = "test_output.parquet"
        self.transformer = RentsTransformer(self.input_file, self.output_file)

    def test_init(self):
        assert self.transformer.input_file == self.input_file
        assert self.transformer.output_file == self.output_file

    @patch('polars.scan_csv')
    def test_transform_success(self, mock_scan):
        mock_lf = Mock()
        mock_scan.return_value = mock_lf
        mock_lf.with_columns.return_value = mock_lf
        sample = Mock()
        sample.height = 2
        sample.columns = ["ANNUAL_AMOUNT", "AREA_EN"]
        sample.__getitem__ = Mock(return_value=Mock(null_count=Mock(return_value=0)))
        mock_lf.head.return_value.collect.return_value = sample
        mock_lf.collect_schema.return_value.names.return_value = []
        mock_lf.sink_parquet.return_value = None
        assert self.transformer.transform() is True
        mock_lf.sink_parquet.assert_called_once()

    @patch('polars.scan_csv')
    def test_transform_file_not_found(self, mock_scan):
        mock_scan.side_effect = FileNotFoundError("File not found")
        assert self.transformer.transform() is False


class TestPropertyUsage:
    def setup_method(self):
        """Setup for each test method."""
        self.output_file = "test_property_usage.csv"
        self.property_usage = PropertyUsage(self.output_file)
    
    @patch('polars.scan_parquet')
    def test_transform_success(self, mock_scan):
        """Test successful property usage transformation."""
        # Mock the lazy frame with test data
        mock_lf = Mock()
        mock_scan.return_value = mock_lf
        
        # Create test dataframe with proper structure
        test_df = pl.DataFrame({
            'property_usage_en': ['Residential', 'Commercial', 'Residential'],
            'no_of_contracts': [10, 5, 15],
            'avg_rent': [50000, 75000, 60000],
            'median_rent': [48000, 72000, 58000],
            'min_rent': [30000, 50000, 35000],
            'max_rent': [80000, 120000, 90000],
            'std_rent': [15000, 25000, 18000]
        })
        
        # Mock the filter and group_by operations
        mock_filtered = Mock()
        mock_lf.filter.return_value = mock_filtered
        mock_filtered.with_columns.return_value = mock_filtered  # Support with_columns chaining
        mock_filtered.group_by.return_value = mock_filtered
        mock_filtered.agg.return_value = mock_filtered
        mock_filtered.collect.return_value = test_df
        
        # Mock collect_schema
        mock_schema = Mock()
        mock_schema.names.return_value = ['property_usage_en', 'annual_amount']
        mock_lf.collect_schema.return_value = mock_schema
        
        # Mock write_csv
        with patch.object(pl.DataFrame, 'write_csv'):
            self.property_usage.transform("test_input.parquet")
    
    @patch('polars.scan_parquet')
    def test_transform_with_area_data(self, mock_scan):
        """Test transformation with area data available."""
        mock_lf = Mock()
        mock_scan.return_value = mock_lf
        
        # Mock schema with area data
        mock_schema = Mock()
        mock_schema.names.return_value = ['property_usage_en', 'annual_amount', 'actual_area']
        mock_lf.collect_schema.return_value = mock_schema
        
        # Create test dataframe with proper structure
        test_df = pl.DataFrame({
            'property_usage_en': ['Residential'],
            'no_of_contracts': [10],
            'avg_rent': [50000],
            'median_rent': [48000],
            'min_rent': [30000],
            'max_rent': [80000],
            'std_rent': [15000]
        })
        
        # Prepare specific dataframes for joins
        size_df = pl.DataFrame({
            'property_usage_en': ['Residential'],
            'avg_area_sqft': [1000.0],
            'median_area_sqft': [950.0]
        })
        
        psf_df = pl.DataFrame({
            'property_usage_en': ['Residential'],
            'avg_psf': [50.0],
            'median_psf': [48.0]
        })
        
        mock_filtered = Mock()
        mock_lf.filter.return_value = mock_filtered
        mock_filtered.with_columns.return_value = mock_filtered  # Support with_columns chaining
        mock_filtered.group_by.return_value = mock_filtered
        mock_filtered.agg.return_value = mock_filtered
        # Return main df first, then size stats, then psf stats
        mock_filtered.collect.side_effect = [test_df, size_df, psf_df]
        
        with patch.object(pl.DataFrame, 'write_csv'):
            self.property_usage.transform("test_input.parquet")


class TestValidators:
    def setup_method(self):
        """Setup for each test method."""
        self.validator = RentContractValidator(strict_mode=False)
        
        # Create test dataframe with proper date types
        self.test_df = pl.DataFrame({
            'contract_id': [1, 2, 3, 4],
            'contract_start_date': [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1), None],
            'contract_end_date': [date(2024, 12, 31), date(2024, 2, 1), date(2024, 3, 2), None],
            'property_usage_en': ['Residential', 'Commercial', 'Residential', None],
            'annual_amount': [50000.0, 75000.0, -1000.0, None],
            'actual_area': [1000.0, 1500.0, 0.0, None]
        })
    
    def test_validation_result_init(self):
        """Test ValidationResult initialization."""
        from lib.classes.validators import ValidationResult
        result = ValidationResult()
        
        assert result.errors == []
        assert result.warnings == []
        assert result.info == []
        assert result.is_valid is True
    
    def test_validation_result_add_error(self):
        """Test adding error to ValidationResult."""
        from lib.classes.validators import ValidationResult
        result = ValidationResult()
        
        result.add_error("Test error")
        
        assert len(result.errors) == 1
        assert result.errors[0] == "Test error"
        assert result.is_valid is False
    
    def test_validation_result_get_summary(self):
        """Test ValidationResult summary."""
        from lib.classes.validators import ValidationResult
        result = ValidationResult()
        
        result.add_error("Test error")
        result.add_warning("Test warning")
        result.add_info("Test info")
        
        summary = result.get_summary()
        
        assert summary['errors'] == 1
        assert summary['warnings'] == 1
        assert summary['info'] == 1
        assert summary['is_valid'] is False
    
    def test_validate_dataframe_success(self):
        """Test successful dataframe validation."""
        result = self.validator.validate_dataframe(self.test_df)
        
        assert result is not None
        assert len(result.info) > 0
    
    def test_validate_dataframe_empty(self):
        """Test validation of empty dataframe."""
        empty_df = pl.DataFrame(schema=self.test_df.schema)
        result = self.validator.validate_dataframe(empty_df)
        
        assert result.is_valid is False
        assert len(result.errors) == 1
        assert "DataFrame is empty" in result.errors[0]
    
    def test_validate_required_fields(self):
        """Test required fields validation."""
        # Test with missing required field
        df_missing_field = self.test_df.drop('contract_id')
        result = self.validator.validate_dataframe(df_missing_field)
        
        assert not result.is_valid
        assert any("Missing required columns" in error for error in result.errors)
    
    def test_validate_rent_amounts(self):
        """Test rent amount validation."""
        result = self.validator.validate_dataframe(self.test_df)
        
        # Should detect negative rent
        assert any("rent <= 0" in error for error in result.errors)
    
    def test_validate_business_logic(self):
        """Test business logic validation."""
        # Create dataframe with invalid date range
        df_invalid_dates = pl.DataFrame({
            'contract_id': [1],
            'contract_start_date': [date(2024, 1, 1)],
            'contract_end_date': [date(2023, 1, 1)],  # End before start
            'property_usage_en': ['Residential'],
            'annual_amount': [50000.0]
        })
        
        result = self.validator.validate_dataframe(df_invalid_dates)
        
        assert any("end_date <= start_date" in error for error in result.errors)
    
    def test_validate_rent_contracts_function(self):
        """Test convenience function."""
        result = validate_rent_contracts(self.test_df, strict=False)
        
        assert result is not None
        assert hasattr(result, 'get_summary')


class TestETLPipelineIntegration:
    """Integration tests for the complete ETL pipeline."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.test_url = "https://example.com/test"
        self.csv_filename = "test_rent_contracts.csv"
        self.parquet_filename = "test_rent_contracts.parquet"
        self.property_usage_report = "test_property_usage.csv"
    
    @patch.dict(os.environ, {'EJARI_URL': 'https://example.com/test'})
    @patch('run_etl_pipeline.EjariRentsDownloader')
    @patch('run_etl_pipeline.RentsTransformer')
    @patch('run_etl_pipeline.PropertyUsage')
    @patch('run_etl_pipeline.GitHubRelease')
    def test_complete_pipeline_success(self, mock_github_class, mock_property_usage_class,
                                     mock_transformer_class, mock_downloader_class):
        """Test complete ETL pipeline execution."""
        # Setup mocks

        mock_downloader = Mock()
        mock_downloader.run.return_value = True
        mock_downloader_class.return_value = mock_downloader
        mock_transformer = Mock()
        mock_transformer.transform.return_value = True
        mock_transformer_class.return_value = mock_transformer
        mock_property_usage = Mock()
        mock_property_usage_class.return_value = mock_property_usage
        mock_publisher = Mock()
        mock_github_class.return_value = mock_publisher
        
        # Import and run main function (getsize mocked: repo output/ state must not leak into the test)
        with patch('run_etl_pipeline.os.path.isfile', return_value=False):
            with patch('run_etl_pipeline.os.path.getsize', side_effect=FileNotFoundError):
                with patch('run_etl_pipeline.logger'):
                    from run_etl_pipeline import main
                    assert main() is True
        
        # Verify all components were called
        mock_downloader.run.assert_called_once()
        mock_transformer.transform.assert_called_once()
        # mock_publisher.publish.assert_called_once()
    

    
    @patch.dict(os.environ, {}, clear=True)
    def test_pipeline_missing_env_vars(self):
        """Test pipeline with missing environment variables."""
        with patch('run_etl_pipeline.logger'):
            from run_etl_pipeline import main
            assert main() is False
        
    @patch.dict(os.environ, {'EJARI_URL': 'https://example.com/test'})
    @patch('run_etl_pipeline.EjariRentsDownloader')
    def test_pipeline_download_failure_returns_false(self, mock_downloader_class):
        mock_downloader_class.return_value.run.return_value = False

        with patch('run_etl_pipeline.os.path.isfile', return_value=False):
            with patch('run_etl_pipeline.logger'):
                from run_etl_pipeline import main
                assert main() is False
    
    def test_download_rent_contracts_file_exists(self):
        """Test download function when file already exists."""
        with patch('run_etl_pipeline.os.path.isfile', return_value=True):
            with patch('run_etl_pipeline.logger'):
                from run_etl_pipeline import download_rents
                download_rents(self.test_url, self.csv_filename)

    @patch('run_etl_pipeline.EjariRentsDownloader')
    def test_download_rent_contracts_new_file(self, mock_downloader_class):
        """Test download function for new file."""
        mock_downloader = Mock()
        mock_downloader.run.return_value = True
        mock_downloader_class.return_value = mock_downloader

        with patch('run_etl_pipeline.os.path.isfile', return_value=False):
            with patch('run_etl_pipeline.logger'):
                from run_etl_pipeline import download_rents
                download_rents(self.test_url, self.csv_filename)

        mock_downloader_class.assert_called_once_with(self.test_url)
        mock_downloader.run.assert_called_once_with(self.csv_filename)
    
    @patch.dict(os.environ, {'GH_TOKEN': 'test_token'})
    @patch('run_etl_pipeline.GitHubRelease')
    @patch('run_etl_pipeline.os.path.exists', return_value=True)
    @patch('run_etl_pipeline.os.path.getsize', return_value=1024)
    def test_publish_to_github_release_success(self, mock_getsize, mock_exists, mock_github_class):
        """Test successful GitHub publish function."""
        mock_publisher = Mock()
        mock_github_class.return_value = mock_publisher
        
        test_files = [self.parquet_filename, self.property_usage_report]
        
        with patch('run_etl_pipeline.logger'):
            from run_etl_pipeline import publish_artifacts_to_github
            publish_artifacts_to_github(test_files)
        
        mock_github_class.assert_called_once_with('dataengineergaurav/rental-market-dynamics-dubai')
        mock_publisher.publish.assert_called_once_with(files=test_files, tag_name=None)
