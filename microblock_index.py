# from collections import defaultdict
# from blockmetadata import BlockMetadata
# import pyarrow.parquet as pq

# class MicroBlockIndex:
#     def __init__(self):
#         self.index = []
#         self.by_column = defaultdict(list)

#     def add_block(self, block):
#         self.index.append(block)
#         self.by_column[(block.table_id, block.column_id)].append(block)

#     def build_from_parquet(self, file_path, table_id="t1"):
#         pf = pq.ParquetFile(file_path)

#         running_row_start = 0

#         for rg in range(pf.num_row_groups):
#             rg_meta = pf.metadata.row_group(rg)
#             row_count = rg_meta.num_rows

#             row_start = running_row_start
#             row_end = running_row_start + row_count - 1
#             running_row_start = row_end + 1

#             # for each column in this row group
#             for col_id in range(rg_meta.num_columns):
#                 col_meta = rg_meta.column(col_id)

#                 stats = None
#                 if col_meta.statistics is not None:
#                     stats = {
#                         "min": col_meta.statistics.min,
#                         "max": col_meta.statistics.max,
#                         "null_count": col_meta.statistics.null_count
#                     }

#                 block = BlockMetadata(
#                     table_id=table_id,
#                     column_id=col_id,
#                     file_path=file_path,
#                     row_group_id=rg,
#                     row_start=row_start,
#                     row_end=row_end,
#                     byte_offset=col_meta.dictionary_page_offset or col_meta.data_page_offset,
#                     byte_length=col_meta.total_compressed_size,
#                     statistics=stats,
#                     compression_info=str(col_meta.compression)
#                 )

#                 self.add_block(block)

#         return self



import time
from collections import defaultdict
import pyarrow.parquet as pq


class BlockMetadata:
    def __init__(
        self,
        table_id,
        column_name,
        column_id,
        file_path,
        row_group_id,
        row_start,
        row_end,
        byte_offset,
        byte_length,
        statistics,
        compression_info,
    ):
        self.table_id = table_id
        self.column_id = column_id
        self.column_name = column_name

        self.file_path = file_path
        self.row_group_id = row_group_id
        self.row_start = row_start
        self.row_end = row_end

        self.byte_offset = byte_offset
        self.byte_length = byte_length

        # statistics is a dict like {"min": ..., "max": ..., "null_count": ...}
        self.statistics = statistics
        self.compression_info = compression_info

        # usage counters for future ml and caching
        self.access_count = 0
        self.last_access_ts = 0
        self.ewma_usage = 0.0
        self.ewma_alpha = 0.2

    def mark_access(self):
        now = time.time()
        self.access_count += 1
        self.last_access_ts = now
        self.ewma_usage = self.ewma_alpha * 1.0 + (1 - self.ewma_alpha) * self.ewma_usage


class MicroBlockIndex:
    def __init__(self):
        # flat list of all blocks
        self.blocks = []
        # map (table_id, column_name) -> list of blocks
        self.by_column = defaultdict(list)
        # map (table_id, row_group_id) -> dict column_name -> BlockMetadata
        self.by_row_group = defaultdict(dict)
        # self.index = []

    def add_block(self, block: BlockMetadata):
        self.blocks.append(block)
        self.by_column[(block.table_id, block.column_name)].append(block)
        self.by_row_group[(block.table_id, block.row_group_id)][block.column_name] = block

    def build_from_parquet(self, file_path, table_id="t1"):
        pf = pq.ParquetFile(file_path)
        schema = pf.schema

        running_row_start = 0

        for rg in range(pf.num_row_groups):
            rg_meta = pf.metadata.row_group(rg)
            row_count = rg_meta.num_rows

            row_start = running_row_start
            row_end = running_row_start + row_count - 1
            running_row_start = row_end + 1

            for col_idx in range(rg_meta.num_columns):
                col_meta = rg_meta.column(col_idx)
                col_name = schema.names[col_idx]

                stats = None
                if col_meta.statistics is not None:
                    s = col_meta.statistics
                    stats = {
                        "min": s.min,
                        "max": s.max,
                        "null_count": s.null_count,
                    }

                byte_offset = (
                    col_meta.dictionary_page_offset
                    if col_meta.dictionary_page_offset is not None
                    else col_meta.data_page_offset
                )

                block = BlockMetadata(
                    table_id=table_id,
                    column_name=col_name,
                    column_id=col_idx,
                    file_path=file_path,
                    row_group_id=rg,
                    row_start=row_start,
                    row_end=row_end,
                    byte_offset=byte_offset,
                    byte_length=col_meta.total_compressed_size,
                    statistics=stats,
                    compression_info=str(col_meta.compression),
                )

                self.add_block(block)

        return self

    def stats_for_row_group(self, table_id, row_group_id):
        """
        returns a dict column_name -> statistics dict for that row group
        """
        col_map = self.by_row_group.get((table_id, row_group_id), {})
        out = {}
        for col_name, block in col_map.items():
            if block.statistics is not None:
                out[col_name] = block.statistics
        return out
