from dataclasses import dataclass
from datetime import datetime

from rich.progress import TaskID


@dataclass
class LoadComplete:
    cluster_name: str
    loaded_datetime: datetime
    metrics: dict[str, str | None]
    is_end: bool
    task_id: TaskID


@dataclass
class AggregateComplete:
    task_id: TaskID
    is_end: bool
    is_empty: bool
