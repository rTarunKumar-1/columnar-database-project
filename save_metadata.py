# import pandas as pd
# from microblock_writer import MicroBlockWriter
# from microblock_index import MicroBlockIndex
# from microblock_reader import MicroBlockReader

# df = pd.DataFrame({
#     "id": range(100000),
#     "value": range(100000)
# })

# writer = MicroBlockWriter(block_size=16384)
# writer.write(df, "data_micro.parquet")

# index = MicroBlockIndex().build_from_parquet("data_micro.parquet")

# print("total blocks:", len(index.index))
# for b in index.index[:3]:
#     print("block:", b.block_id, "range:", b.row_start, "-", b.row_end)

# reader = MicroBlockReader(index)

# result = reader.scan_range(30000, 35000)

# print("result rows:", result.num_rows)
# print(result.to_pandas().head())


from tabulate import tabulate
import duckdb

df = duckdb.sql("""
    SELECT * 
    FROM parquet_metadata('E:/microblock_storage/output.parquet')
""").df()

with open("metadata_table.txt", "w", encoding="utf8") as f:
    f.write(tabulate(df, headers="keys", tablefmt="psql"))

df = duckdb.sql("""
    SELECT * 
    FROM parquet_metadata('E:\microblock_storage\output_microblocks.parquet')
""").df()

with open("metadata_table_microblock.txt", "w", encoding="utf8") as f:
    f.write(tabulate(df, headers="keys", tablefmt="psql"))

