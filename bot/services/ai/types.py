from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pandas as pd

from bot.services.excel_24h import SnapshotMeta
from bot.services.warehouse_delay import WarehouseDelayAggregationResult

AISourceKind = Literal[
    "uploaded_file",
    "yadisk_no_move_latest",
    "project_24h",
    "yadisk_warehouse_latest",
]
AIExtractedKind = Literal["table", "no_move", "h24", "warehouse_delay"]


@dataclass(frozen=True)
class LLMMessage:
    role: Literal["system", "user", "assistant"]
    content: str


@dataclass(frozen=True)
class AILimits:
    max_files_per_request: int
    max_file_bytes: int
    max_rows_per_source: int
    max_scan_rows_per_source: int
    max_context_chars: int
    max_history_messages: int
    max_answer_chars: int
    max_retries: int
    retry_backoff_ms: int


@dataclass(frozen=True)
class AI24hSnapshotContent:
    snapshot: dict[str, dict[str, object]]
    meta: SnapshotMeta


@dataclass(frozen=True)
class AIWarehouseContent:
    aggregation: WarehouseDelayAggregationResult


AIExtractedContent = pd.DataFrame | AI24hSnapshotContent | AIWarehouseContent


@dataclass(frozen=True)
class AISourceRef:
    kind: AISourceKind
    label: str
    filename: str | None = None
    file_path: Path | None = None


@dataclass(frozen=True)
class AIExtractionResult:
    source_ref: AISourceRef
    extracted_kind: AIExtractedKind
    display_name: str
    content: AIExtractedContent
    rows_scanned: int
    was_truncated: bool = False
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class AIContextBlock:
    title: str
    text: str
    rows_scanned: int
    rows_selected: int
    applied_limits: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    was_truncated: bool = False


@dataclass(frozen=True)
class AIContextPackage:
    text: str
    blocks: tuple[AIContextBlock, ...]
    total_rows_scanned: int
    total_rows_selected: int
    truncation_count: int
    source_types: tuple[str, ...]


@dataclass(frozen=True)
class AIProviderResult:
    text: str
    model: str
    attempt_count: int
    latency_ms: int


@dataclass(frozen=True)
class AIAssistantResult:
    answer_text: str
    model: str
    source_labels: tuple[str, ...]
    total_rows_scanned: int
    total_rows_selected: int
    context_chars: int
    truncation_count: int
    provider_attempt_count: int
    provider_latency_ms: int
    notes: tuple[str, ...] = ()


@dataclass
class AISessionState:
    active: bool = False
    source_refs: list[AISourceRef] = field(default_factory=list)
    history: list[dict[str, str]] = field(default_factory=list)
    uploaded_paths: list[str] = field(default_factory=list)
