"""Read-only incremental watermark support.

Step 3 reads the last successfully committed watermark when one exists. The
watermark is intentionally not advanced until the later quality-gated step.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


@dataclass(frozen=True)
class Watermark:
    updated_at: datetime
    order_id: int

    def __post_init__(self) -> None:
        if self.updated_at.tzinfo is None:
            raise ValueError("Watermark updated_at must include a timezone")
        if self.order_id < 0:
            raise ValueError("Watermark order_id cannot be negative")

    @property
    def utc_updated_at(self) -> datetime:
        return self.updated_at.astimezone(timezone.utc)


def load_watermark(path: Path) -> Watermark | None:
    if not path.exists():
        return None

    with path.open(encoding="utf-8") as state_file:
        state = json.load(state_file)

    try:
        updated_at = datetime.fromisoformat(state["updated_at"].replace("Z", "+00:00"))
        order_id = int(state["order_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid watermark file: {path}") from exc

    return Watermark(updated_at=updated_at, order_id=order_id)


def save_watermark(
    path: Path,
    watermark: Watermark,
    *,
    batch_id: str,
) -> None:
    """Atomically publish a monotonic compound watermark."""
    current = load_watermark(path)
    candidate_key = (watermark.utc_updated_at, watermark.order_id)
    if current:
        current_key = (current.utc_updated_at, current.order_id)
        if candidate_key < current_key:
            raise ValueError("Refusing to move the incremental watermark backwards")

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp_{uuid4().hex}"
    state = {
        "updated_at": watermark.utc_updated_at.isoformat().replace("+00:00", "Z"),
        "order_id": watermark.order_id,
        "batch_id": batch_id,
        "committed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    try:
        with temporary.open("w", encoding="utf-8") as state_file:
            json.dump(state, state_file, indent=2, sort_keys=True)
            state_file.write("\n")
            state_file.flush()
            os.fsync(state_file.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
