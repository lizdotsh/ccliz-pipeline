from dataclasses import dataclass, field
from os import path
from typing import Optional

from .types import CCRecordStage, CCRecordURL, LocalConfig
from .utils import process_segment_url, stage_converter


@dataclass
class CCRecord:
    snapshot: str
    segment: str
    file_num: str
    raw: str
    record_id: str
    config: LocalConfig
    stage: CCRecordStage = CCRecordStage.VOID
    stage_history: list[CCRecordStage] = field(default_factory=list)

    # pipeline: CCPipeline = None
    @staticmethod
    def create_from_URL(
        url: str,
        config: LocalConfig = LocalConfig(
            cc_path="CC",
            # Downloader=None,
            URL_Appendix="default",
            stage_converter=stage_converter,
        ),
        **kwargs,
    ) -> "CCRecord":
        """Creates a CCRecord from a URL"""
        parsed_URL: CCRecordURL = process_segment_url(url)
        return CCRecord(
            snapshot=parsed_URL["snapshot"],
            segment=parsed_URL["segment"],
            file_num=parsed_URL["file_num"],
            raw=parsed_URL["raw"],
            record_id=f"{parsed_URL['snapshot']}/{parsed_URL['segment']}/{parsed_URL['file_num']}",
            config=config,
            **kwargs,
        )

    def get_path(self, stage: Optional[CCRecordStage]) -> str:
        if not stage:
            stage = self.stage
        if not isinstance(stage, CCRecordStage):
            raise ValueError("stage must be of type CCRecordStage")
        if stage == CCRecordStage.ERROR:
            return self.get_path(self.stage_history[-1])
        file_path = path.join(
            self.config["cc_path"],
            self.config["stage_converter"][stage][0],
            self.record_id + self.config + self.stage_converter[stage][1],
        )
        return file_path

    def get_dir(self, stage: CCRecordStage | None = None) -> str | None:
        if not stage:
            stage = self.stage
        return path.dirname(self.get_path(stage))

    def update_stage(self, stage: CCRecordStage):
        self.stage_history.append(self.stage)
        self.stage = stage

    def __repr__(self):
        return f"""
    CCRecord:
        record_id = {self.record_id}
        stage = {self.stage}
        stage_history = {self.stage_history}
        snapshot = {self.snapshot}
        segment = {self.segment}
        file_num = {self.file_num}
        raw = {self.raw}
        """
