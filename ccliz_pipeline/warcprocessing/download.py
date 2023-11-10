import gzip
import os
import re
from os import path
from shutil import copyfileobj

import requests
import tqdm

from .types import ArchiveIO, CCRecord


def create_local_dirs(
    record: CCRecord,
    CC_path: str = "CC/",
    local_stages: list[str] = ["prepared", "filtered", "deduplicated", "final"],
):
    stages = ["staging", "source"] + [
        path.join("local", stage) for stage in local_stages
    ]
    prefix_without_num = record["record_id_prefix"].split(os.path.sep)[0:-1]
    dirs = [path.join(CC_path, ext, *prefix_without_num) for ext in stages]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


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
