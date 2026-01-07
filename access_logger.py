import time
from collections import deque
from dataclasses import dataclass
from typing import List
import json
import os


@dataclass
class AccessEvent:
    timestamp: float
    block: int


class AccessLogger:
    # Logs one event per accessed block to access_log.json

    def __init__(self, path: str = "access_log.json"):
        self.path = path
        self.events: List[dict] = []

        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    self.events = json.load(f)
            except Exception:
                self.events = []

    def log(self, row_groups: List[int]):
        ts = time.time()
        for rg in row_groups:
            event = {"ts": ts, "block": int(rg)}
            self.events.append(event)
        self._flush()

    def _flush(self):
        with open(self.path, "w") as f:
            json.dump(self.events, f)

    def get_all_events(self) -> List[dict]:
        return self.events

    def get_last_n_events(self, n: int) -> List[dict]:
        return self.events[-n:]


class GlobalHistory:
    # Maintains a rolling history of block ids.
    def __init__(self, maxlen: int = 200):
        self.history = deque(maxlen=maxlen)

    def record(self, block_id: int):
        self.history.append(int(block_id))

    def get_sequence(self, length: int | None = None) -> List[int]:
        if length is None or length >= len(self.history):
            return list(self.history)
        return list(self.history)[-length:]

    def clear(self):
        self.history.clear()
