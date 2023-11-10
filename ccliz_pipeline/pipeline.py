from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import Deque, Protocol, TypedDict

from .warcprocessing.types import CCRecord, CCRecordStage


@dataclass
class CCPipeline:
    void: Deque[CCRecord] = deque()
    source: Deque[CCRecord] = deque()
    staging: Deque[CCRecord] = deque()
    preprocessing: Deque[CCRecord] = deque()
    preprocessed: Deque[CCRecord] = deque()
    filtering: Deque[CCRecord] = deque()
    filtered: Deque[CCRecord] = deque()
    deduplicating: Deque[CCRecord] = deque()
    deduplicated: Deque[CCRecord] = deque()
    final: Deque[CCRecord] = deque()
    error: Deque[CCRecord] = deque()
