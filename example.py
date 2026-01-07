from microblock_index import MicroBlockIndex
from tabulate import tabulate

file_path = "output.parquet"

index = MicroBlockIndex().build_from_parquet(file_path, table_id="mytable")

# print("total blocks:", len(index.blocks))

# for b in index.blocks[:5]:
#     print("block")
#     print(" table:", b.table_id)
#     print(" col:", b.column_id)
#     print(" row_group:", b.row_group_id)
#     print(" row range:", b.row_start, b.row_end)
#     print(" offset:", b.byte_offset)
#     print(" size:", b.byte_length)
#     print(" stats:", b.statistics)
#     print(" compression:", b.compression_info)
#     print()


from query_engine import StorageEngine

engine = StorageEngine("output.parquet", table_name="mytable")

sql = "select * from mytable where column1 between 18 and 24"
df = engine.query(sql)

print(df.head())

with open("result.txt", "w", encoding="utf8") as f:
    f.write(tabulate(df, headers="keys", tablefmt="psql"))
