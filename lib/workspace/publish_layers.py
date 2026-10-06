"""Publish the combined layers DuckDB to its own stable release tag.

This is the single publishing path for the layers store — it uses the same `GitHubRelease`
client (`lib.workspace.github_client`) as the daily bronze publish, which clobbers existing
assets so the tag always carries the newest file.

Usage:
  uv run python -m lib.workspace.publish_layers --artifact output/rents_layers.duckdb
"""
from __future__ import annotations

import argparse
from pathlib import Path
import logging

from lib.workspace.github_client import GitHubRelease

logger = logging.getLogger(__name__)

DEFAULT_REPO = "dataengineergaurav/rental-market-dynamics-dubai"
DEFAULT_TAG = "release-layers-latest"


def publish_layers(
    artifact_path: str | Path,
    repo: str = DEFAULT_REPO,
    tag: str = DEFAULT_TAG,
    data_through: str | None = None,
) -> str:
    """Publish the combined DuckDB to `tag` (default: the stable `release-layers-latest`).

    A missing artifact fails loudly before any upload, so the tag is never created without its
    asset. The GitHubRelease client reuses an existing release for the tag and deletes the
    same-named asset first, so a same-day re-run refreshes idempotently. Returns the tag.
    """
    artifact = Path(artifact_path)
    if not artifact.exists():
        raise FileNotFoundError(f"layers artifact not found: {artifact}")

    body = "Combined Silver+Gold DuckDB (cumulative)."
    if data_through:
        body += f" Data through {data_through}."
    GitHubRelease(repo).publish(
        files=[str(artifact)],
        tag_name=tag,
        name="layers latest" + (f" (data through {data_through})" if data_through else ""),
        body=body,
    )
    return tag


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish the combined layers DuckDB")
    parser.add_argument("--artifact", required=True, help="path to the layers DuckDB")
    parser.add_argument("--tag", default=DEFAULT_TAG, help=f"release tag (default {DEFAULT_TAG})")
    parser.add_argument("--data-through", help="max registration date, for the release notes")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="owner/repo to publish to")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    tag = publish_layers(
        artifact_path=args.artifact,
        repo=args.repo,
        tag=args.tag,
        data_through=args.data_through,
    )
    logger.info(f"Published {tag}")


if __name__ == "__main__":
    main()
