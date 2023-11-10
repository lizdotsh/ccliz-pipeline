from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from os import path
from typing import Deque, Optional, Protocol, TypedDict
from uuid import UUID

from msgspec import Struct


class WARCHeader(Struct):
    warc_record_id: UUID
    iso_timestamp: str
    block_digest: str
    payload_digest: str
    ip_address: str
    target_uri: str
    content_type: str
    content_length: int
    identified_payload_type: str
    warc_type: str


class TextDocument(Struct):
    id: str  # "CC/YYYY-mm/[0000-9999]/[integer]"
    header: WARCHeader
    raw_text: str  # "raw" text (only minimal processing)
    pipeline_status: str  # "raw",


class CCRecordStage(Enum):
    VOID = auto()
    SOURCE = auto()  # only if local file
    STAGED = auto()
    PREPROCESSING = auto()
    PREPROCESSED = auto()
    FILTERING = auto()
    FILTERED = auto()
    DEDUPLICATING = auto()
    DEDUPLICATED = auto()
    FINAL = auto()
    ERROR = auto()


class ArchiveIO(Protocol):
    def get(snapshot: str, segment: str):
        ...

    def __call__(self, snapshot: str, segment: str):
        ...


class LocalConfig(TypedDict):
    cc_path: str
    Downloader: ArchiveIO


@dataclass
class CCRecord:
    snapshot: str
    segment: str
    file_num: str
    raw: str
    record_id: str
    stage: CCRecordStage
    config: LocalConfig
    stage_history: list[CCRecordStage] = []

    # pipeline: CCPipeline = None

    @staticmethod
    def _rem_ext(path_str: str | None) -> str | None:
        """Removes last extension from path"""
        if not path_str:
            return path_str
        splits = path_str.split(path.sep)
        return path.join(*splits[:-1])

    def get_path(self, stage: Optional[CCRecordStage]) -> str:
        if not stage:
            stage = self.stage
        # match by stage
        file_path = self._match_stage(stage)
        return file_path

    def _match_stage(self, stage: CCRecordStage) -> str:
        match stage:
            case CCRecordStage.VOID:
                return ""
            case CCRecordStage.SOURCE:
                return path.join(
                    self.config["cc_path"], "source", self.record_id + ".warc.gz"
                )
            case CCRecordStage.STAGED | CCRecordStage.PREPROCESSING:
                return path.join(
                    self.config["cc_path"], "staging", self.record_id + ".warc"
                )
            case CCRecordStage.PREPROCESSED | CCRecordStage.FILTERING:
                return path.join(
                    self.config["cc_path"], "local/prepared", self.record_id + ".jsonl"
                )
            case CCRecordStage.FILTERED | CCRecordStage.DEDUPLICATING:
                return path.join(
                    self.config["cc_path"], "local/filtered", self.record_id + ".jsonl"
                )
            case CCRecordStage.DEDUPLICATED:
                return path.join(
                    self.config["cc_path"],
                    "local/deduplicated",
                    self.record_id + ".jsonl",
                )
            case CCRecordStage.FINAL:
                return path.join(
                    self.config["cc_path"], "local/final", self.record_id + ".jsonl"
                )
            case CCRecordStage.ERROR:
                return self._match_stage(self.stage_history[-1])

    def get_dir(self, stage: CCRecordStage | None = None) -> str | None:
        if not stage:
            stage = self.stage
        return self._rem_ext(self._match_stage(stage))

    def update_stage(self, stage: CCRecordStage):
        self.stage_history.append(self.stage)
        self.stage = stage


class ArchiveHandler(Protocol):
    def get(snapshot: str, segment: str):
        ...

    def __call__(self, stream, filehandler, snapshot_date, segment):
        ...


# status_info:
