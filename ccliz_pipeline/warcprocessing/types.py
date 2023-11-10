from typing import Protocol, TypedDict
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


class CCRecord(TypedDict):
    snapshot: str
    segment: str
    file_num: str
    raw: str
    record_id_prefix: str


class ArchiveHandler(Protocol):
    def get(snapshot: str, segment: str):
        ...

    def __call__(self, stream, filehandler, snapshot_date, segment):
        ...


class ArchiveIO(Protocol):
    def get(snapshot: str, segment: str):
        ...

    def __call__(self, snapshot: str, segment: str):
        ...


# status_info:
