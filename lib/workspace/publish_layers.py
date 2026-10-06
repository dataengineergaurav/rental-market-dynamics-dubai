"""Publish the combined layers DuckDB into the day's release.

There is one release per data date (`release-YYYY-MM-DD`) — the same tag the daily bronze CSV is
published to — and it carries the raw CSV, `etl_status.json` and the cumulative `rents_layers.duckdb`
together. This module adds the DuckDB to that release via the shared `GitHubRelease` client, which
reuses an existing release for the tag (the bronze job usually created it first) and clobbers any
same-named asset.

Usage:
  uv run python -m lib.workspace.publish_layers --artifact output/rents_layers.duckdb --data-through 2026-10-05
"""
from __future__ import annotations

import argparse
from pathlib import Path
import logging

from lib.workspace.github_client import GitHubRelease

logger = logging.getLogger(__name__)

DEFAULT_REPO = "dataengineergaurav/rental-market-dynamics-dubai"


def publish_layers(
    artifact_path: str | Path,
    repo: str = DEFAULT_REPO,
    tag: str | None = None,
    data_through: str | None = None,
) -> str:
    """Publish the combined DuckDB to the day's release tag.

    The tag is `release-<data_through>` unless one is passed explicitly. A missing artifact fails
    loudly before any upload. The release is not named/annotated here: it is the same release the
    bronze job creates, so it keeps its name/body and simply gains this asset. Returns the tag.
    """
    artifact = Path(artifact_path)
    if not artifact.exists():
        raise FileNotFoundError(f"layers artifact not found: {artifact}")

    if tag is None:
        if not data_through:
            raise ValueError("publish_layers needs --tag or --data-through (to derive release-<date>)")
        tag = f"release-{data_through}"

    GitHubRelease(repo).publish(files=[str(artifact)], tag_name=tag)
    return tag


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish the combined layers DuckDB into the day's release")
    parser.add_argument("--artifact", required=True, help="path to the layers DuckDB")
    parser.add_argument("--tag", help="release tag (default: release-<data-through>)")
    parser.add_argument("--data-through", help="max registration date YYYY-MM-DD; derives the tag")
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
