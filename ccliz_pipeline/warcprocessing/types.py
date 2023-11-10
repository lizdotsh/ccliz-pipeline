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


class LocalConfig(TypedDict):
    cc_path: str
    #   Downloader: Optional[ArchiveIO]
    URL_Appendix: str
    stage_converter: dict[CCRecordStage, tuple[str, str]]


class ArchiveHandler(Protocol):
    def get(snapshot: str, segment: str):
        ...

    def __call__(self, stream, filehandler, snapshot_date, segment):
        ...


# status_info:
