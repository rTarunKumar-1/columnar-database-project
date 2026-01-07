# block_id_mapper.py

from dataclasses import dataclass
import pyarrow.parquet as pq


@dataclass(frozen=True)
class BlockIdMapper:

    # block_id == row_group_id in the Parquet file
    num_blocks: int

    @classmethod
    def from_parquet(cls, parquet_path: str) -> "BlockIdMapper":
        pf = pq.ParquetFile(parquet_path)
        return cls(num_blocks=pf.num_row_groups)

    def to_block_id(self, row_group_id: int) -> int:
        # Map a Parquet row_group_id to a model-facing block_id.
        if not (0 <= row_group_id < self.num_blocks):
            raise ValueError(f"row_group_id {row_group_id} out of range 0..{self.num_blocks - 1}")
        return row_group_id

    def to_row_group_id(self, block_id: int) -> int:
        # Map a model block_id back to a Parquet row_group_id.
        if not (0 <= block_id < self.num_blocks):
            raise ValueError(f"block_id {block_id} out of range 0..{self.num_blocks - 1}")
        return block_id
