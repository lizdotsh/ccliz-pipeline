import logging as log
import multiprocessing as mp
from functools import partial
from os import makedirs, path
from typing import BinaryIO, Literal

import msgspec
from fastwarc.stream_io import FileStream, GZipStream
from fastwarc.warc import ArchiveIterator, WarcRecordType
from tqdm import tqdm

from .preprocessing import preprocess_raw_bytes, preprocessing_rules
from .types import CCRecord, CCRecordStage, TextDocument
from .utils import check_and_makedirs, check_file, make_warc_header

encoder = msgspec.json.Encoder()


def stream_from_cc_file(path_to_cc_file: str, streamHandler):
    if path_to_cc_file.endswith(".gz"):
        with GZipStream(FileStream(path_to_cc_file)) as stream:
            return streamHandler(stream)
    with FileStream(path_to_cc_file) as stream:
        return streamHandler(stream)


def warc_record_handler(document: WarcRecordType, id: int, record: CCRecord):
    header = make_warc_header(document.headers)
    body = preprocess_raw_bytes(document.reader.read())
    if not preprocessing_rules(body):
        # if id % 25 == 0:
        #    print("failed preprocessing rules", body)
        return None
    document_id = path.join(record.record_id, str(id))
    log.info(f"Created document {document_id} ")
    return TextDocument(
        id=document_id,
        header=header,
        raw_text=body,
        pipeline_status="raw",
    )


def handle_archive_stream(stream, filehandler: BinaryIO, record: CCRecord):
    # ret = []
    buffer = bytearray(2000000)
    buffer.clear()
    count_all = 0
    count_passed = 0
    for id, document in tqdm(
        enumerate(
            ArchiveIterator(
                stream,
                parse_http=False,
                record_types=WarcRecordType.response,
            )
        )
    ):
        rec = warc_record_handler(document, id, record)
        count_all += 1

        if not rec:
            continue
        encoder.encode_into(rec, buffer, -1)
        buffer.extend(b"\n")
        if len(buffer) > 1500000:
            log.info(f"{record.record_id} ({id}): writing buffer", len(buffer))
            count_passed += 1

            filehandler.write(buffer)
            buffer.clear()


def managed_stream(
    output_file: str,
    path_to_cc_file: str,
    snapshot: str,
    segment: str,
    overwrite: bool = True,
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
        stream_from_cc_file(
            path_to_cc_file,
            partial(
                handle_archive_stream,
                filehandler=file,
                snapshot_date=snapshot,
                segment=segment,
            ),
        )


def batch_managed_streams(
    output_file: str,
    cc_files: list[str],
    cc_segments_per_file: list[str],
    cc_snapshot: str,
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


def process_record(
    record: CCRecord, overwrite: Literal["always", "never", "rename"] = "rename"
):
    if record.stage != CCRecordStage.STAGED:
        raise ValueError(f"Record {record.record_id} is not staged")
    try:
        record.update_stage(CCRecordStage.PREPROCESSING)
        log.info(f"Processing record {record.record_id}")
        source_file_path = check_file(record.get_path(CCRecordStage.STAGED), overwrite)
        processed_file_path = check_and_makedirs(
            record.get_path(CCRecordStage.PREPROCESSED)
        )
        with open(processed_file_path, "wb") as file:
            stream_from_cc_file(
                source_file_path,
                partial(
                    handle_archive_stream,
                    filehandler=file,
                    record=record,
                ),
            )
        # print("stage", record.stage)
    except Exception as e:
        log.error(f"Error processing record {record.record_id}: {e}")
        record.update_stage(CCRecordStage.ERROR)
        return record
    else:
        log.info(f"Finished processing record {record.record_id}")
        record.update_stage(CCRecordStage.PREPROCESSED)
        return record
