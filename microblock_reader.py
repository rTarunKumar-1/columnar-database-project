import pyarrow.parquet as pq
import pyarrow as pa

class MicroBlockReader:
    def __init__(self, index):
        self.index = index

    def scan_range(self, row_start, row_end):
        blocks = self.index.find_blocks_for_range(row_start, row_end)
        tables = []

        for block in blocks:
            pf = pq.ParquetFile(block.file_path)
            table = pf.read_row_group(block.block_id)
            tables.append(table)

        if len(tables) == 0:
            return pa.Table.from_arrays([])
        return pa.concat_tables(tables)
