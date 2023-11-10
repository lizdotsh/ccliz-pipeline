import re
import unicodedata
from os import makedirs, path, remove
from typing import Literal, Match
from uuid import UUID

import trafilatura as tf
from fastwarc.warc import WarcHeaderMap

from .types import CCRecord, CCRecordStage, TextDocument, WARCHeader


def process_html(str: str):
    return tf.extract(str)


def make_warc_header(record: WarcHeaderMap) -> WARCHeader:
    rectuple = record.astuples()

    return WARCHeader(
        warc_record_id=UUID(rectuple[2][1][1:-1]),
        iso_timestamp=rectuple[1][1],
        block_digest=rectuple[10][1],
        payload_digest=rectuple[9][1],
        ip_address=rectuple[7][1],
        target_uri=rectuple[8][1],
        content_type=rectuple[4][1],
        content_length=rectuple[3][1],
        identified_payload_type=rectuple[11][1],
        warc_type=rectuple[0][1],
    )


def check_file(
    filePath,
    overwrite: Literal["always", "rename", "never"] = "rename",
):
    if not path.exists(filePath):
        return filePath
    match overwrite:
        case "always":
            remove(filePath)
            return filePath
        case "never":
            raise FileExistsError(f"File {filePath} already exists")
        case "rename":
            return _rename_file(filePath)


def _rename_file(filePath):
    numb = 1
    while True:
        newPath = "{0}_{2}{1}".format(*path.splitext(filePath) + (numb,))
        if path.exists(newPath):
            numb += 1
        else:
            return newPath


def check_and_makedirs(path_with_extension: str) -> str:
    file_path = check_file(path_with_extension)
    if not path.exists(path.dirname(file_path)):
        makedirs(path.dirname(file_path))
    return file_path


process_segment_url_re = re.compile(
    r"crawl-data\/CC-MAIN-(\d{4}-\d{2})\/segments\/(\d+\.\d+)\/warc\/CC-MAIN-\d{14}-\d{14}-(\d{5f})\.warc\.gz"
)


def process_segment_url(
    url: str,
) -> Match[str]:
    match = process_segment_url_re.search(url)
    if not match:
        raise ValueError(f"Could not process {url}")

    return match
