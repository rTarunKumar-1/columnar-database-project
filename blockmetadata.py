import time

class BlockMetadata:
    def __init__(
        self,
        table_id,
        column_id,
        file_path,
        row_group_id,
        row_start,
        row_end,
        byte_offset,
        byte_length,
        statistics=None,
        compression_info=None
    ):
        # ids
        self.table_id = table_id
        self.column_id = column_id

        # location
        self.file_path = file_path
        self.row_group_id = row_group_id
        self.row_start = row_start
        self.row_end = row_end

        # offsets
        self.byte_offset = byte_offset
        self.byte_length = byte_length

        # from parquet
        self.statistics = statistics
        self.compression_info = compression_info

        # usage counters
        self.access_count = 0
        self.last_access_ts = 0
        self.ewma_usage = 0.0
        self.ewma_alpha = 0.2

    def mark_access(self):
        now = time.time()
        self.access_count += 1
        self.last_access_ts = now
        self.ewma_usage = (
            self.ewma_alpha * 1.0 +
            (1 - self.ewma_alpha) * self.ewma_usage
        )
