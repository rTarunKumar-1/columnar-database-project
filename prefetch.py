# prefetch.py

import pyarrow.parquet as pq
from typing import Optional
from block_cache import BlockCache


class Prefetcher:
    """
    Responsible for turning ML predictions into real prefetched blocks.
    Uses PyArrow to load row groups and stores them in a BlockCache.
    """

    def __init__(self, parquet_path: str, cache: BlockCache):
        self.parquet_path = parquet_path
        self.pf = pq.ParquetFile(parquet_path)
        self.cache = cache

    def prefetch_block(self, block_id: int) -> bool:
        """
        Prefetch the given microblock (row_group_id) into cache.

        Returns:
            True if prefetched successfully.
            False if block already in cache or error.
        """
        if self.cache.contains(block_id):
            print(f"[Prefetcher] block {block_id} already in cache, skipping")
            return False

        try:
            table = self.pf.read_row_group(block_id)
            self.cache.put(block_id, table)
            print(f"[Prefetcher] prefetched block {block_id}")
            return True

        except Exception as e:
            print(f"[Prefetcher] prefetch error for block {block_id}: {e}")
            return False

    def prefetch_many(self, block_ids):
        for bid in block_ids:
            self.prefetch_block(bid)
