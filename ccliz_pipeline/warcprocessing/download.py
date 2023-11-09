import msgspec
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator, WarcRecordType
from tqdm import tqdm

from .types import TextDocument, WARCHeader
from .utils import make_warc_header

encoder = msgspec.json.Encoder()

CC_SNAPSHOT = "2023-09"
CC_SEGMENT = "00000"


def stream_from_cc_file(path_to_cc_file: str, streamHandler):
    with GZipStream(FileStream(path_to_cc_file)) as stream:
        return streamHandler(stream)


def warc_record_handler(
    record: WarcRecordType, id: int, snapshot_date: str, segment: int
):
    header = make_warc_header(record.headers)
    return TextDocument(
        id=f"CC/{snapshot_date}/{segment}/{id}",
        header=header,
        raw_content=record.reader.read(),
        pipeline_status="raw",
    )


def handle_archive_stream(stream):
    ret = []
    for id, record in enumerate(
        ArchiveIterator(
            stream,
            parse_http=True,
            record_types=WarcRecordType.response,
            #  func_filter=lambda r: r.headers.get("WARC-Identified-Payload-Type")
            #  == "text/html",
        )
    ):
        ret.append(warc_record_handler(record, id, CC_SNAPSHOT, CC_SEGMENT))
    return ret


# def processResponse(record: WarcRecordType.response)
