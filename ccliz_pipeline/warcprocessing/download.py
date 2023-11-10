import gzip
import logging as log
import os
import re
from os import path
from shutil import copyfileobj
from typing import Literal

import requests
import tqdm

from .pipeline import CCRecord
from .types import ArchiveIO, CCRecordStage
from .utils import check_and_makedirs, check_file

process_segment_url_re = re.compile(
    r"crawl-data\/CC-MAIN-(\d{4}-\d{2})\/segments\/(\d+\.\d+)\/warc\/CC-MAIN-\d{14}-\d{14}-(\d{5})\.warc\.gz"
)


def process_segment_url(url: str) -> CCRecord:
    match = process_segment_url_re.search(url)
    if not match:
        raise ValueError(f"Could not process {url}")

    return CCRecord(
        snapshot=match.group(1),
        segment=match.group(2),
        file_num=match.group(3),
        raw=url,
        record_id_prefix=f"{match.group(1)}/{match.group(2)}/{match.group(3)}",
    )


class HTTPArchiveIO(ArchiveIO):
    def __init__(
        self,
        # record: CCRecord,
        CC_local_path: str = "CC/",
        CC_remote_path: str = "https://data.commoncrawl.org/",
    ):
        # self.record = record
        self.CC_local_path = CC_local_path
        self.CC_remote_path = CC_remote_path

    def _format_url(self, record: CCRecord) -> str:
        return path.join(self.CC_remote_path, record["raw"])

    def download(self, record: CCRecord):
        # Download the file to the local path
        req = requests.get(record["raw"], allow_redirects=True)


class LocalArchiveIO(ArchiveIO):
    """
    This is really messy honestly. need to fix asap
    """

    def __init__(
        self,
        # record: CCRecord,
        CC_local_path: str = "CC/local/",
        CC_staging_path: str = "CC/staging/",
        CC_source_path: str = "CC/source/",
    ):
        os.makedirs(CC_local_path, exist_ok=True)
        os.makedirs(CC_staging_path, exist_ok=True)
        # os.makedirs(CC_source_path, exist_ok=True)

        if not path.exists(CC_local_path):
            raise ValueError(f"CC_local_path {CC_local_path} does not exist")
        if not path.exists(CC_source_path):
            raise ValueError(f"CC_source_path {CC_source_path} does not exist")
        if not path.exists(CC_staging_path):
            raise ValueError(f"CC_staging_path {CC_staging_path} does not exist")
        # self.record = record
        self._local = CC_local_path
        self._staging = CC_staging_path
        self._source = CC_source_path

    # def _format_url(self, record: CCRecord) -> str:
    #     return path.join(self.CC_remote_path, record["raw"])
    def stage(self, record: CCRecord, delete=False, overwrite=False):
        # Copy file in CCRecord.url to staging

        splits = (record["record_id_prefix"] + ".warc.gz").split(os.path.sep)
        if not path.isfile(path.join(self._source, *splits)):
            raise ValueError(f"File {path.join(self._source, *splits)} does not exist")
        os.makedirs(path.join(self._staging, *splits[:-1]), exist_ok=True)
        self._copy_file_gzip(
            path.join(self._source, *splits),
            path.join(self._staging, *splits),
            delete=delete,
            overwrite=overwrite,
        )
        return path.join(self._staging, *splits)

    @staticmethod
    def _copy_file_gzip(source, dest, delete: bool = False, overwrite: bool = False):
        if path.exists(dest):
            if overwrite:
                os.remove(dest)
            else:
                raise ValueError(f"File {dest} already exists")
        with gzip.open(source, "rb") as f_in:
            with open(dest, "wb") as f_out:
                copyfileobj(f_in, f_out)
        if delete:
            os.remove(source)


def _copy_file_gzip(
    source, dest, delete: bool = False, overwrite: Literal["always", "never"] = "never"
):
    source = check_file(source, overwrite)
    dest = check_and_makedirs(dest)
    with gzip.open(source, "rb") as f_in:
        with open(dest, "wb") as f_out:
            copyfileobj(f_in, f_out)
    if delete:
        os.remove(source)
    return dest


def stage_record(
    record: CCRecord, overwrite: Literal["always", "never"] = "never", delete=False
):
    if record.stage != CCRecordStage.SOURCE:
        raise ValueError(
            f"Record {record.record_id} is not ready to be staged/isn't sourced"
        )
    try:
        log.info(f"Staging record {record.record_id}")
        source_path = record.get_path(record, CCRecordStage.SOURCE)
        staged_path = record.get_path(record, CCRecordStage.STAGED)
        dest = _copy_file_gzip(
            source_path, staged_path, delete=delete, overwrite=overwrite
        )
    except Exception as e:
        log.error(f"Error staging record {record.record_id}")
        record.update_stage(CCRecordStage.ERROR)
        raise e
    else:
        record.update_stage(CCRecordStage.STAGED)
        return dest
