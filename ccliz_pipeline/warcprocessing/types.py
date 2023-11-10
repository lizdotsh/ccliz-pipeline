from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from os import path
from typing import Deque, Optional, Protocol, TypedDict
from uuid import UUID

from msgspec import Struct

from .utils import process_segment_url


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


class CCRecordURL(TypedDict):
    snapshot: str
    segment: str
    file_num: str
    raw: str


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


stage_converter = {
    CCRecordStage.VOID: ("", ""),
    CCRecordStage.SOURCE: ("source", ".warc.gz"),
    CCRecordStage.STAGED: ("staging", ".warc"),
    CCRecordStage.PREPROCESSING: ("staging", ".warc"),
    CCRecordStage.PREPROCESSED: ("local/prepared", ".jsonl"),
    CCRecordStage.FILTERING: ("local/prepared", ".jsonl"),
    CCRecordStage.FILTERED: ("local/filtered", ".jsonl"),
    CCRecordStage.DEDUPLICATING: ("local/filtered", ".jsonl"),
    CCRecordStage.DEDUPLICATED: ("local/deduplicated", ".jsonl"),
    CCRecordStage.FINAL: ("local/final", ".jsonl"),
}


class LocalConfig(TypedDict):
    cc_path: str
    Downloader: Optional[ArchiveIO]
    URL_Appendix: str
    stage_converter: dict[CCRecordStage, tuple[str, str]]


@dataclass
class CCRecord:
    snapshot: str
    segment: str
    file_num: str
    raw: str
    record_id: str
    stage: CCRecordStage = CCRecordStage.VOID
    config: LocalConfig() = LocalConfig(
        cc_path="CC",
        Downloader=None,
        URL_Appendix="test_nov10",
        stage_converter=stage_converter,
    )
    stage_history: list[CCRecordStage] = []

    # pipeline: CCPipeline = None
    @staticmethod
    def create_from_URL(
        url: str,
        **kwargs,
    ) -> "CCRecord":
        """Creates a CCRecord from a URL"""
        parsed_URL: CCRecordURL = process_segment_url(url)
        return CCRecord(
            snapshot=parsed_URL["snapshot"],
            segment=parsed_URL["segment"],
            file_num=parsed_URL["file_num"],
            raw=parsed_URL["raw"],
            record_id=f"{parsed_URL['snapshot']}/{parsed_URL['segment']}/{parsed_URL['file_num']}",
            **kwargs,
        )

    def get_path(self, stage: Optional[CCRecordStage]) -> str:
        if not stage:
            stage = self.stage
        if not isinstance(stage, CCRecordStage):
            raise ValueError("stage must be of type CCRecordStage")
        if stage == CCRecordStage.ERROR:
            return self._match_stage(self.stage_history[-1])
        file_path = path.join(
            self.config["cc_path"],
            self.stage_converter[stage][0],
            self.record_id + self.config + self.stage_converter[stage][1],
        )
        return file_path

    def get_dir(self, stage: CCRecordStage | None = None) -> str | None:
        if not stage:
            stage = self.stage
        return path.dirname(self.get_path(stage))

    def update_stage(self, stage: CCRecordStage):
        self.stage_history.append(self.stage)
        self.stage = stage


class ArchiveHandler(Protocol):
    def get(snapshot: str, segment: str):
        ...

    def __call__(self, stream, filehandler, snapshot_date, segment):
        ...


# status_info:
