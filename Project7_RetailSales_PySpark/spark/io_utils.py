"""Safe local publication helpers for Parquet snapshot datasets."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import DataFrame


def publish_snapshots(datasets: dict[Path, DataFrame], publication_id: str) -> None:
    """Write all temporary datasets before replacing any published snapshot."""
    workspaces: dict[Path, tuple[Path, Path]] = {}
    try:
        for target, dataframe in datasets.items():
            temporary = target.parent / f".{target.name}_tmp_{publication_id}"
            backup = target.parent / f".{target.name}_backup_{publication_id}"
            if temporary.exists() or backup.exists():
                raise FileExistsError(
                    f"Publication workspace already exists for {target}"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            workspaces[target] = (temporary, backup)
            dataframe.coalesce(1).write.mode("errorifexists").parquet(str(temporary))
    except Exception:
        for temporary, _ in workspaces.values():
            if temporary.exists():
                shutil.rmtree(temporary)
        raise

    published_targets: list[Path] = []
    try:
        for target, (_, backup) in workspaces.items():
            if target.exists():
                os.replace(target, backup)

        for target, (temporary, _) in workspaces.items():
            os.replace(temporary, target)
            published_targets.append(target)
    except Exception:
        for target in published_targets:
            if target.exists():
                shutil.rmtree(target)
        for target, (_, backup) in workspaces.items():
            if backup.exists() and not target.exists():
                os.replace(backup, target)
        raise
    else:
        for _, backup in workspaces.values():
            if backup.exists():
                shutil.rmtree(backup)
    finally:
        for temporary, _ in workspaces.values():
            if temporary.exists():
                shutil.rmtree(temporary)


def publish_snapshot(dataframe: DataFrame, target: Path, publication_id: str) -> None:
    publish_snapshots({target: dataframe}, publication_id)
