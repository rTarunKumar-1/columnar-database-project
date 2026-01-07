from collections import OrderedDict
from typing import Any, Optional


class BlockCache:
    #  memory LRU cache for prefetched microblocks.
    # Mapping:
    #     block_id -> PyArrow Table
    # On insertion:
    # If block_id already exists, update position to MRU.
    # If cache is full, evict LRU block.

    def __init__(self, capacity: int = 64):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, block_id: int) -> Optional[Any]:
        # Retrieve a block from the cache.
        # Move to MRU (most recently used) position.
        # Returns:PyArrow Table or None if not present.
        if block_id not in self.cache:
            return None

        self.cache.move_to_end(block_id, last=True)
        return self.cache[block_id]

    def put(self, block_id: int, block_data: Any):
        # If block exists:
        #     update and move to MRU.

        # If at capacity:
        #     evict LRU block.
        # if block already present, replace and move to MRU
        if block_id in self.cache:
            self.cache.move_to_end(block_id, last=True)
            self.cache[block_id] = block_data
            return

        if len(self.cache) >= self.capacity:
            evicted_block_id, _ = self.cache.popitem(last=False)
            # print("evicted", evicted_block_id)

        # insert MRU
        self.cache[block_id] = block_data

    def contains(self, block_id: int) -> bool:
        return block_id in self.cache

    def remove(self, block_id: int):
        self.cache.pop(block_id, None)

    def clear(self):
        self.cache.clear()

    def __len__(self):
        return len(self.cache)

    def stats(self):
        return {
            "capacity": self.capacity,
            "size": len(self.cache),
            "cached_blocks": list(self.cache.keys()),
        }
