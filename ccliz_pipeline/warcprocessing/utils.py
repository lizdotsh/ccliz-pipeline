import re
import unicodedata
from uuid import UUID

import trafilatura as tf
from fastwarc.warc import WarcHeaderMap

from .types import TextDocument, WARCHeader


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
