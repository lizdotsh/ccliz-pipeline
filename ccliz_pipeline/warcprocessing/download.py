from os import makedirs, path
from typing import BinaryIO

import msgspec
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.parse.encoding import bytes_to_str
from tqdm import tqdm

from .preprocessing import preprocess_raw_bytes
from .types import TextDocument, WARCHeader
from .utils import make_warc_header

encoder = msgspec.json.Encoder()

CC_SNAPSHOT = "2023-09"
CC_SEGMENT = "00000"


def stream_from_cc_file(path_to_cc_file: str, streamHandler):
    with GZipStream(FileStream(path_to_cc_file)) as stream:
        return streamHandler(stream)


def warc_record_handler(
    record: WarcRecordType, id: int, snapshot_date: str, segment: str
):
    header = make_warc_header(record.headers)

    return TextDocument(
        id=f"CC/{snapshot_date}/{segment}/{id}",
        header=header,
        raw_text=preprocess_raw_bytes(record.reader.read()),
        pipeline_status="raw",
    )


def oldfashioned_handle_archive_stream(stream):
    ret = []
    for id, record in enumerate(
        ArchiveIterator(
            stream,
            parse_http=True,
            record_types=WarcRecordType.response,
        )
    ):
        ret.append(warc_record_handler(record, id, CC_SNAPSHOT, CC_SEGMENT))
    return ret


def handle_archive_stream(stream, filehandler: BinaryIO):
    # ret = []
    buffer = bytearray(2000000)
    buffer.clear()
    for id, record in tqdm(
        enumerate(
            ArchiveIterator(
                stream,
                parse_http=True,
                record_types=WarcRecordType.response,
                #  func_filter=lambda r: r.headers.get("WARC-Identified-Payload-Type")
                #  == "text/html",
            )
        )
    ):
        # filehandler.write(
        encoder.encode_into(
            warc_record_handler(record, id, CC_SNAPSHOT, CC_SEGMENT), buffer
        )
        buffer.extend(b"\n")
        filehandler.write(buffer)
        buffer.clear()


def managed_stream(output_file: str, path_to_cc_file: str, overwrite: bool = True):
    # write line by line to output_file
    # mkdir if not exists
    output_path = path.join("CC", CC_SNAPSHOT, CC_SEGMENT)
    if not path.exists(output_path):
        makedirs(output_path)
    # if overwrite, delete file else error
    if path.exists(path.join(output_path, output_file)):
        if overwrite:
            print(f"Overwriting {output_file}")
        else:
            raise FileExistsError(f"{output_file} exists")
    with open(path.join(output_path, output_file), "wb") as file:
        stream_from_cc_file(path_to_cc_file, lambda st: handle_archive_stream(st, file))


# def processResponse(record: WarcRecordType.response)
