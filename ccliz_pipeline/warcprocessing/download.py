import multiprocessing as mp
from os import makedirs, path
from typing import BinaryIO

import msgspec
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator, WarcRecordType
from tqdm import tqdm

from .preprocessing import preprocess_raw_bytes, preprocessing_rules
from .types import TextDocument
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
    body = preprocess_raw_bytes(record.reader.read())
    if not preprocessing_rules(body):
        # if id % 25 == 0:
        #    print("failed preprocessing rules", body)
        return None
    return TextDocument(
        id=f"CC/{snapshot_date}/{segment}/{id}",
        header=header,
        raw_text=body,
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
    count_all = 0
    count_passed = 0
    for id, record in tqdm(
        enumerate(
            ArchiveIterator(
                stream,
                parse_http=False,
                record_types=WarcRecordType.response,
            )
        )
    ):
        # if id % 10 == 0:
        #    print(id, count_all, count_passed)
        rec = warc_record_handler(record, id, CC_SNAPSHOT, CC_SEGMENT)
        count_all += 1

        if not rec:
            continue
        encoder.encode_into(rec, buffer, -1)
        buffer.extend(b"\n")
        if len(buffer) > 1500000:
            print("writing buffer", len(buffer))
            count_passed += 1

            filehandler.write(buffer)
            buffer.clear()


def managed_stream(
    output_file: str,
    path_to_cc_file: str,
    overwrite: bool = True,
    snapshot: str = CC_SNAPSHOT,
    segment: str = CC_SEGMENT,
):
    # write line by line to output_file
    # mkdir if not exists
    print(f"Writing to {output_file} with snapshot {snapshot} and segment {segment}\n")
    output_path = path.join("CC", snapshot, segment)
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


def batch_managed_streams(
    output_file: str,
    cc_files: list[str],
    cc_segments_per_file: list[str],
    cc_snapshot: str = CC_SNAPSHOT,
    overwrite: bool = True,
):
    # dispatch managed_stream for each segment

    with mp.Pool(
        processes=mp.cpu_count() - 2,
    ) as pool:
        pool.starmap(
            managed_stream,
            [
                (
                    output_file,
                    cc_file,
                    overwrite,
                    cc_snapshot,
                    cc_segment,
                )
                for cc_file, cc_segment in zip(cc_files, cc_segments_per_file)
            ],
        )
