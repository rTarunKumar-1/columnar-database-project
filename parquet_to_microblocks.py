import duckdb
import pyarrow.parquet as pq

# input and output paths
input_file = "E:/microblock_storage/output.parquet"
output_file = "E:/microblock_storage/output_microblocks.parquet"

print("converting to micro-block format...")

# convert parquet file with custom row-group size
duckdb.sql(f""" COPY ( SELECT * FROM '{input_file}') TO '{output_file}' ( FORMAT 'parquet', ROW_GROUP_SIZE 16384); """)


# verify output row groups
pf = pq.ParquetFile(output_file)
num_groups = pf.num_row_groups

print(f"total microblocks created: {num_groups}")

for i in range(min(5, num_groups)):  # print first few blocks
    rg = pf.metadata.row_group(i)
    print(f"block {i} - rows: {rg.num_rows} - total size: {rg.total_byte_size} bytes")
