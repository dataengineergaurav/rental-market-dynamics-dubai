from dotenv import load_dotenv
import requests
from datetime import date
import logging
import os

load_dotenv()
logger = logging.getLogger(__name__)

GITHUB_API_URL = "https://api.github.com"

class GitHubRelease:
    """
    A class to handle publishing releases to a GitHub repository.

    Attributes:
        repo (str): The GitHub repository in the format 'owner/repo'.
        token (str): The GitHub token for authentication.
        headers (dict): The headers for GitHub API requests.

    Methods:
        create_release():
            Creates a new release in the specified GitHub repository.
        
        upload_files(release, files):
            Uploads files to the specified GitHub release.
        
        publish(files):
            Creates a new release and uploads the specified files to it.

    Usage:
        publisher = GitHubReleasePublisher(repo="owner/repo")
        publisher.publish(files=["file1.txt", "file2.zip"])
    """
    def __init__(self, repo):
        self.repo = repo
        self.token = os.getenv("GH_TOKEN")
        if not self.token:
            logger.error("GH_TOKEN not found in environment variables")
            raise ValueError("GH_TOKEN not set")
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }

    def create_release(self, tag_name=None, name=None, body=None):
        tag_name = tag_name or f"release-{date.today()}"
        release_name = name or tag_name.replace("release-", "Release ", 1)

        release_data = {
            "tag_name": tag_name,
            "name": release_name,
            "body": body or "Automated release",
            "draft": False,
            "prerelease": False
        }

        try:
            response = requests.post(f"{GITHUB_API_URL}/repos/{self.repo}/releases", headers=self.headers, json=release_data)
            response.raise_for_status()
            release = response.json()
            logger.info(f"Created GitHub release {release_name}")
            return release
        except requests.exceptions.RequestException as e:
            logger.error(f"GitHub API error: {e}")
            raise

    def _delete_asset_named(self, release, name):
        """Delete an existing asset with this name so it can be re-uploaded.

        The upload endpoint returns 422 when an asset of the same name already exists, so a
        same-day re-run of the daily job could not refresh its CSV. Deleting first makes the
        upload idempotent (the weekly path already uses `gh release upload --clobber`).
        """
        assets_url = release.get("assets_url")
        if not assets_url and release.get("id"):
            assets_url = f"{GITHUB_API_URL}/repos/{self.repo}/releases/{release['id']}/assets"
        if not assets_url:
            return
        resp = requests.get(assets_url, headers=self.headers)
        if resp.status_code != 200:
            return
        for asset in resp.json():
            if asset.get("name") == name:
                delete = requests.delete(asset["url"], headers=self.headers)
                delete.raise_for_status()
                logger.info(f"Deleted existing asset {name} (id {asset.get('id')}) to re-upload")

    def upload_files(self, release, files):
        for file in files:
            name = os.path.basename(file)
            try:
                self._delete_asset_named(release, name)

                with open(file, 'rb') as f:
                    upload_url = release['upload_url'].split('{')[0] + f"?name={name}"
                    upload_headers = self.headers.copy()
                    upload_headers["Content-Type"] = "application/octet-stream"

                    upload_response = requests.post(upload_url, headers=upload_headers, data=f)
                    upload_response.raise_for_status()
                    logger.info(f"Uploaded {file} to GitHub release {release['name']}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to upload {file}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error uploading {file}: {e}")

    def publish(self, files, tag_name=None, name=None, body=None):
        try:
            tag_name = tag_name or f"release-{date.today()}"
            if self.release_exists(tag_name):
                # reuse existing release for incremental re-runs same day
                resp = requests.get(f"{GITHUB_API_URL}/repos/{self.repo}/releases/tags/{tag_name}", headers=self.headers)
                resp.raise_for_status()
                release = resp.json()
                logger.info(f"Reusing existing release {tag_name}")
            else:
                release = self.create_release(tag_name, name=name, body=body)
            self.upload_files(release, files)
        except Exception as e:
            logger.error(f"Failed to publish release: {e}")
    

    def release_exists(self, tag_name):
        """
        Checks if a release with the specified tag name already exists.

        Args:
            tag_name (str): The tag name of the release to check.

        Returns:
            bool: True if the release exists, False otherwise.
        """
        response = requests.get(f"{GITHUB_API_URL}/repos/{self.repo}/releases/tags/{tag_name}", headers=self.headers)
        if response.status_code == 200:
            logger.info(f"Release with tag {tag_name} already exists.")
            return True
        if response.status_code == 404:
            logger.info(f"No release found with tag {tag_name}.")
            return False