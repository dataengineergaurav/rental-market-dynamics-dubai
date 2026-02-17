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
from lib.extract.rent_contracts_downloader import RentContractsDownloader
from lib.transform.rent_contracts_transformer import RentContractsTransformer, StarSchema
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


class TestRentContractsDownloader:
    def setup_method(self):
        """Setup for each test method."""
        self.test_url = "https://example.com/test"
        self.downloader = RentContractsDownloader(self.test_url)
    
    @patch('lib.extract.rent_contracts_downloader.requests.get')
    def test_fetch_rent_contracts_success(self, mock_get):
        """Test successful HTML fetch."""
        mock_response = Mock()
        mock_response.content = b"<html><body>Test content</body></html>"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = self.downloader.fetch_rent_contracts()
        assert result == b"<html><body>Test content</body></html>"
        mock_get.assert_called_once_with(self.test_url, timeout=30)
    
    @patch('lib.extract.rent_contracts_downloader.requests.get')
    def test_fetch_rent_contracts_retry(self, mock_get):
        """Test retry logic on failure."""
        mock_response = Mock()
        mock_response.content = b"<html><body>Test content</body></html>"
        mock_response.raise_for_status.return_value = None
        mock_get.side_effect = [requests.exceptions.Timeout("Timeout"), mock_response]
        
        with patch('time.sleep'):
            result = self.downloader.fetch_rent_contracts()
            assert result == b"<html><body>Test content</body></html>"
            assert mock_get.call_count == 2
    
    def test_parse_html_success(self):
        """Test successful HTML parsing."""
        html_content = b'<html><body><a class="action-icon-anchor" href="download.csv">Download</a></body></html>'
        result = self.downloader.parse_html(html_content)
        assert result == "download.csv"
    
    def test_parse_html_no_link(self):
        """Test HTML parsing when no download link found."""
        html_content = b'<html><body>No download link here</body></html>'
        result = self.downloader.parse_html(html_content)
        assert result is None
    
    @patch('lib.extract.rent_contracts_downloader.requests.get')
    def test_download_file_success(self, mock_get, tmp_path):
        """Test successful file download."""
        mock_response = Mock()
        mock_response.iter_content.return_value = [b"test", b"content"]
        mock_response.raise_for_status.return_value = None
        mock_response.headers = {'content-length': '11'}
        mock_get.return_value = mock_response
        
        file_path = tmp_path / "test_file.csv"
        self.downloader.download_file("http://example.com/file.csv", str(file_path))
        
        assert file_path.exists()
        assert file_path.read_text() == "testcontent"
    
    @patch('lib.extract.rent_contracts_downloader.requests.get')
    def test_download_file_retry(self, mock_get, tmp_path):
        """Test retry logic on file download failure."""
        mock_response = Mock()
        mock_response.iter_content.return_value = [b"test", b"content"]
        mock_response.raise_for_status.return_value = None
        mock_response.headers = {'content-length': '11'}
        mock_get.side_effect = [requests.exceptions.Timeout("Timeout"), mock_response]
        
        file_path = tmp_path / "test_file.csv"
        with patch('time.sleep'):
            self.downloader.download_file("http://example.com/file.csv", str(file_path))
        
        assert file_path.exists()
        assert file_path.read_text() == "testcontent"
    
    @patch.object(RentContractsDownloader, 'fetch_rent_contracts')
    @patch.object(RentContractsDownloader, 'parse_html')
    @patch.object(RentContractsDownloader, 'download_file')
    def test_run_success(self, mock_download, mock_parse, mock_fetch, tmp_path):
        """Test successful run method."""
        mock_fetch.return_value = b"<html>test</html>"
        mock_parse.return_value = "download.csv"
        
        file_path = tmp_path / "output.csv"
        result = self.downloader.run(str(file_path))
        
        assert result is True
        mock_fetch.assert_called_once()
        mock_parse.assert_called_once_with(b"<html>test</html>")
        mock_download.assert_called_once_with("download.csv", str(file_path))
    
    @patch.object(RentContractsDownloader, 'fetch_rent_contracts')
    @patch.object(RentContractsDownloader, 'parse_html')
    def test_run_no_download_link(self, mock_parse, mock_fetch, tmp_path):
        """Test run method when no download link found."""
        mock_fetch.return_value = b"<html>test</html>"
        mock_parse.return_value = None
        
        file_path = tmp_path / "output.csv"
        result = self.downloader.run(str(file_path))
        
        assert result is False


class TestRentContractsTransformer:
    def setup_method(self):
        """Setup for each test method."""
        self.input_file = "test_input.csv"
        self.output_file = "test_output.parquet"
        self.transformer = RentContractsTransformer(self.input_file, self.output_file, validate=False)
    
    def test_init(self):
        """Test transformer initialization."""
        assert self.transformer.input_file == self.input_file
        assert self.transformer.output_file == self.output_file
        assert self.transformer.validate is False
    
    @patch('polars.scan_csv')
    @patch('polars.DataFrame.write_parquet')
    def test_transform_success(self, mock_write, mock_scan):
        """Test successful transformation."""
        # Mock the lazy frame
        mock_lf = Mock()
        mock_scan.return_value = mock_lf
        
        # Mock the with_columns chain
        mock_lf.with_columns.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = Mock()
        mock_lf.sink_parquet.return_value = None
        
        result = self.transformer.transform()
        
        assert result is True
        mock_scan.assert_called_once()
        mock_lf.sink_parquet.assert_called_once()
    
    @patch('polars.scan_csv')
    def test_transform_file_not_found(self, mock_scan):
        """Test transformation with missing input file."""
        mock_scan.side_effect = FileNotFoundError("File not found")
        
        result = self.transformer.transform()
        
        assert result is False
    
    @patch('polars.scan_csv')
    def test_transform_computation_error(self, mock_scan):
        """Test transformation with polars computation error."""
        mock_lf = Mock()
        mock_scan.return_value = mock_lf
        mock_lf.with_columns.side_effect = pl.exceptions.ComputeError("Compute error")
        
        result = self.transformer.transform()
        
        assert result is False
    
    def test_log_statistics(self):
        """Test statistics logging."""
        # Create test dataframe
        test_df = pl.DataFrame({
            'annual_amount': [50000, 75000, 100000, None],
            'property_usage_en': ['Residential', 'Commercial', 'Residential', 'Commercial']
        })
        
        # This should not raise an exception
        self.transformer._log_statistics(test_df)


class TestStarSchema:
    def setup_method(self):
        """Setup for each test method."""
        self.test_df = pl.DataFrame({
            'property_id': [1, 2, 3],
            'area_name': ['Dubai Marina', 'Downtown Dubai', 'JBR']
        })
        self.test_query = "SELECT * FROM rent_contracts_df WHERE area_name = 'Dubai Marina'"
        self.star_schema = StarSchema(self.test_df, self.test_query)
    
    def test_init(self):
        """Test StarSchema initialization."""
        assert self.star_schema.rent_contracts_df.equals(self.test_df)
        assert self.star_schema.query == self.test_query
    
    def test_transform(self):
        """Test StarSchema transformation."""
        result = self.star_schema.transform()
        
        # Should return filtered dataframe
        assert result.height == 1
        assert result['area_name'][0] == 'Dubai Marina'


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
    
    @patch.dict(os.environ, {'DLD_URL': 'https://example.com/test'})
    @patch('run_etl_pipeline.RentContractsDownloader')
    @patch('run_etl_pipeline.RentContractsTransformer')
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
        
        # Import and run main function
        with patch('run_etl_pipeline.os.path.isfile', return_value=False):
            with patch('run_etl_pipeline.logger'):
                from run_etl_pipeline import main
                main()
        
        # Verify all components were called
        mock_downloader.run.assert_called_once()
        mock_transformer.transform.assert_called_once()
        # mock_publisher.publish.assert_called_once()
    

    
    @patch.dict(os.environ, {}, clear=True)
    def test_pipeline_missing_env_vars(self):
        """Test pipeline with missing environment variables."""
        with patch('run_etl_pipeline.logger'):
            from run_etl_pipeline import main
            main()
        
        # Should log error and return early
    
    def test_download_rent_contracts_file_exists(self):
        """Test download function when file already exists."""
        with patch('run_etl_pipeline.os.path.isfile', return_value=True):
            with patch('run_etl_pipeline.logger'):
                from run_etl_pipeline import download_rent_contracts
                download_rent_contracts(self.test_url, self.csv_filename)
    
    @patch('run_etl_pipeline.RentContractsDownloader')
    def test_download_rent_contracts_new_file(self, mock_downloader_class):
        """Test download function for new file."""
        mock_downloader = Mock()
        mock_downloader.run.return_value = True
        mock_downloader_class.return_value = mock_downloader
        
        with patch('run_etl_pipeline.os.path.isfile', return_value=False):
            with patch('run_etl_pipeline.logger'):
                from run_etl_pipeline import download_rent_contracts
                download_rent_contracts(self.test_url, self.csv_filename)
        
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
        mock_publisher.publish.assert_called_once_with(files=test_files)
