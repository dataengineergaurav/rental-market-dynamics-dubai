from datetime import date
import os
import pytest
import sys
import requests
import polars as pl

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from lib.workspace.github_client import GitHubRelease

import pytest
import requests
import requests_mock

class TestGitHubRelease:
    @classmethod
    def setup_class(cls):
        """Setup resources before any tests run."""
        cls.repo = "test_owner/test_repo"
        cls.github_release = GitHubRelease(cls.repo)
        cls.mock_release = {
            "upload_url": "https://api.github.com/repos/test_owner/test_repo/releases/1/assets{?name,label}",
            "name": "Test Release"
        }
    
    @classmethod
    def teardown_class(cls):
        """Cleanup resources after all tests are completed."""
        cls.github_release = None
    
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
