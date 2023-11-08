from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator, WarcRecordType


def stream_from_cc_file(path_to_cc_file: str, streamHandler):
    with GZipStream(FileStream(path_to_cc_file)) as stream:
        return streamHandler(stream)


def handle_archive_stream(stream):
    ret = []
    for record in ArchiveIterator(
        stream,
        parse_http=True,
        record_types=WarcRecordType.response,
        #  func_filter=lambda r: r.headers.get("WARC-Identified-Payload-Type")
        #  == "text/html",
    ):
        ret.append(
            {
                "record_id": record.record_id,
                "headers": record.headers,
                "read": record.reader.read(),
            }
        )
    return ret


# def processResponse(record: WarcRecordType.response)
