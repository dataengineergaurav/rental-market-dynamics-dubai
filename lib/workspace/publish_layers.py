"""Publish the weekly Silver and Gold layer DuckDBs to their own release tags.

This is the single publishing path for both the daily bronze CSV and the weekly layers — it uses
the same `GitHubRelease` client (`lib.workspace.github_client`), which now clobbers existing assets.
The weekly workflow and `make weekly-publish` call the CLI below, so neither has to reimplement the
tag/asset logic with `gh` (which had already drifted once: it tagged the wrong week).

Usage:
  uv run python -m lib.workspace.publish_layers --week 2026W39 \
      --silver output/silver_2026W39.duckdb --gold output/gold_2026W39.duckdb
"""
from __future__ import annotations

import argparse
from pathlib import Path
import logging

from lib.workspace.github_client import GitHubRelease

logger = logging.getLogger(__name__)

DEFAULT_REPO = "dataengineergaurav/rental-market-dynamics-dubai"


def publish_weekly_layers(
    silver_path: str | Path,
    gold_path: str | Path,
    week: str,
    repo: str = DEFAULT_REPO,
) -> list[str]:
    """Publish each layer to `release-<layer>-<week>`. Returns the tags published.

    A missing artifact fails loudly before any upload, so a tag is never created that does not
    match its asset.
    """
    gh = GitHubRelease(repo)
    layers = [("silver", Path(silver_path)), ("gold", Path(gold_path))]
    missing = [f"{name} artifact not found: {path}" for name, path in layers if not path.exists()]
    if missing:
        # Validate everything BEFORE the first upload, so a missing Gold cannot leave a Silver
        # release published with no counterpart.
        raise FileNotFoundError("; ".join(missing))

    tags: list[str] = []
    for layer, artifact in layers:
        tag = f"release-{layer}-{week}"
        gh.publish(
            files=[str(artifact)],
            tag_name=tag,
            name=f"{layer} {week}",
            body=f"Weekly {layer} layer {week} — {artifact.name}",
        )
        tags.append(tag)
    return tags


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish the weekly Silver and Gold DuckDBs")
    parser.add_argument("--week", required=True, help="ISO week like 2026W39")
    parser.add_argument("--silver", required=True, help="path to silver_YYYYWww.duckdb")
    parser.add_argument("--gold", required=True, help="path to gold_YYYYWww.duckdb")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="owner/repo to publish to")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    tags = publish_weekly_layers(
        silver_path=args.silver, gold_path=args.gold, week=args.week, repo=args.repo
    )
    logger.info(f"Published {', '.join(tags)}")


if __name__ == "__main__":
    main()
