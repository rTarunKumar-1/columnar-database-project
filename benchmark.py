# # # # import pyarrow as pa
# # # # import duckdb
# # # # import time
# # # # from query_enginev4 import StorageEngine

# # # # from tabulate import tabulate

# # # # parquet_file = "output.parquet"
# # # # engine = StorageEngine(parquet_file, table_name="mytable")

# # # # query = "select * from mytable where column1 between 18 and 24"

# # # # # baseline duckdb
# # # # conn = duckdb.connect()

# # # # print("\n--- baseline duckdb full scan ---")
# # # # t1 = time.time()
# # # # df_base = conn.execute(f"""
# # # #     select * from '{parquet_file}' where column1 between 18 and 24
# # # # """).fetchdf()
# # # # t2 = time.time()
# # # # baseline_time = t2 - t1
# # # # print("baseline time:", baseline_time)

# # # # # microblock full load (no pruning)
# # # # # force the engine to load all row-groups
# # # # print("\n--- microblock engine no pruning ---")
# # # # all_row_groups = list(range(engine.pf.num_row_groups))

# # # # t1 = time.time()
# # # # tables = [engine.pf.read_row_group(rg) for rg in all_row_groups]

# # # # full_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

# # # # engine.con.execute("drop table if exists data")
# # # # engine.con.register("data", full_table)

# # # # df_no_prune = engine.con.execute(query.replace("mytable", "data")).df()
# # # # t2 = time.time()
# # # # nopruning_time = t2 - t1
# # # # print("microblock no pruning time:", nopruning_time)

# # # # # microblock with pruning
# # # # print("\n--- microblock engine with pruning ---")
# # # # t1 = time.time()
# # # # df_mb = engine.query(query)
# # # # t2 = time.time()
# # # # microblock_time = t2 - t1
# # # # print("microblock pruning time:", microblock_time)

# # # # # summary
# # # # print("\n--- summary ---")
# # # # summary = [
# # # #     ["baseline duckdb full scan", baseline_time],
# # # #     ["microblock without pruning", nopruning_time],
# # # #     ["microblock with pruning", microblock_time],
# # # # ]
# # # # print(tabulate(summary, headers=["mode", "seconds"], tablefmt="psql"))


# # # import pyarrow as pa
# # # import duckdb
# # # import time
# # # from query_enginev4 import StorageEngine
# # # from tabulate import tabulate

# # # parquet_file = "output.parquet"

# # # # WARM UP CACHE - Run a query first to load metadata and file into cache
# # # print("\n--- warming up cache ---")
# # # warmup_conn = duckdb.connect()
# # # warmup_conn.execute(f"SELECT COUNT(*) FROM '{parquet_file}'").fetchall()
# # # warmup_conn.close()
# # # print("Cache warmed up!")

# # # engine = StorageEngine(parquet_file, table_name="mytable")
# # # query = "select * from mytable where column1 between 18 and 24"

# # # # baseline duckdb
# # # conn = duckdb.connect()
# # # print("\n--- baseline duckdb full scan ---")
# # # t1 = time.time()
# # # df_base = conn.execute(f"""
# # # select * from '{parquet_file}' where column1 between 18 and 24
# # # """).fetchdf()
# # # t2 = time.time()
# # # baseline_time = t2 - t1
# # # print("baseline time:", baseline_time)

# # # # microblock full load (no pruning)
# # # print("\n--- microblock engine no pruning ---")
# # # all_row_groups = list(range(engine.pf.num_row_groups))
# # # t1 = time.time()
# # # tables = [engine.pf.read_row_group(rg) for rg in all_row_groups]
# # # full_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
# # # engine.con.execute("drop table if exists data")
# # # engine.con.register("data", full_table)
# # # df_no_prune = engine.con.execute("select * from data where column1 between 18 and 24").fetchdf()
# # # t2 = time.time()
# # # no_prune_time = t2 - t1
# # # print("microblock no pruning time:", no_prune_time)

# # # # microblock with pruning
# # # print("\n--- microblock engine with pruning ---")
# # # t1 = time.time()
# # # df_mb = engine.query(query)
# # # t2 = time.time()
# # # prune_time = t2 - t1
# # # print("microblock pruning time:", prune_time)

# # # # summary
# # # print("\n--- summary ---")
# # # data = [
# # #     ["baseline duckdb full scan", baseline_time],
# # #     ["microblock without pruning", no_prune_time],
# # #     ["microblock with pruning", prune_time],
# # # ]
# # # print(tabulate(data, headers=["mode", "seconds"], tablefmt="psql"))


# # import pyarrow as pa
# # import duckdb
# # import time
# # from query_enginev4 import StorageEngine
# # from tabulate import tabulate

# # parquet_file = "output.parquet"

# # # Create ONE persistent connection to reuse across all tests
# # conn = duckdb.connect()

# # engine = StorageEngine(parquet_file, table_name="mytable")
# # query = "select * from mytable where column1 between 18 and 24"

# # # baseline duckdb (using same persistent connection)
# # print("\n--- baseline duckdb full scan ---")
# # t1 = time.time()
# # df_base = conn.execute(f"""
# # select * from '{parquet_file}' where column1 between 18 and 24
# # """).fetchdf()
# # t2 = time.time()
# # baseline_time = t2 - t1
# # print("baseline time:", baseline_time)

# # # microblock full load (no pruning)
# # print("\n--- microblock engine no pruning ---")
# # all_row_groups = list(range(engine.pf.num_row_groups))
# # t1 = time.time()
# # tables = [engine.pf.read_row_group(rg) for rg in all_row_groups]
# # full_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
# # engine.con.execute("drop table if exists data")
# # engine.con.register("data", full_table)
# # df_no_prune = engine.con.execute("select * from data where column1 between 18 and 24").fetchdf()
# # t2 = time.time()
# # no_prune_time = t2 - t1
# # print("microblock no pruning time:", no_prune_time)

# # # microblock with pruning
# # print("\n--- microblock engine with pruning ---")
# # t1 = time.time()
# # df_mb = engine.query(query)
# # t2 = time.time()
# # prune_time = t2 - t1
# # print("microblock pruning time:", prune_time)

# # # summary
# # print("\n--- summary ---")
# # data = [
# #     ["baseline duckdb full scan", baseline_time],
# #     ["microblock without pruning", no_prune_time],
# #     ["microblock with pruning", prune_time],
# # ]
# # print(tabulate(data, headers=["mode", "seconds"], tablefmt="psql"))

# # # Clean up
# # conn.close()


# import pyarrow as pa
# import duckdb
# import time
# from query_enginev4 import StorageEngine
# from tabulate import tabulate

# parquet_file = "output.parquet"

# engine = StorageEngine(parquet_file, table_name="mytable")
# query = "select * from mytable where column1 between 18 and 24"

# # CHANGED ORDER: Run microblock with pruning FIRST
# print("\n--- microblock engine with pruning (FIRST) ---")
# t1 = time.time()
# df_mb = engine.query(query)
# t2 = time.time()
# prune_time = t2 - t1
# print("microblock pruning time:", prune_time)

# # baseline duckdb (now runs SECOND)
# conn = duckdb.connect()
# print("\n--- baseline duckdb full scan (SECOND) ---")
# t1 = time.time()
# df_base = conn.execute(f"""
# select * from '{parquet_file}' where column1 between 18 and 24
# """).fetchdf()
# t2 = time.time()
# baseline_time = t2 - t1
# print("baseline time:", baseline_time)

# # microblock full load (no pruning) - runs LAST
# print("\n--- microblock engine no pruning (LAST) ---")
# all_row_groups = list(range(engine.pf.num_row_groups))
# t1 = time.time()
# tables = [engine.pf.read_row_group(rg) for rg in all_row_groups]
# full_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
# engine.con.execute("drop table if exists data")
# engine.con.register("data", full_table)
# df_no_prune = engine.con.execute("select * from data where column1 between 18 and 24").fetchdf()
# t2 = time.time()
# no_prune_time = t2 - t1
# print("microblock no pruning time:", no_prune_time)

# # summary
# print("\n--- summary ---")
# data = [
#     ["microblock with pruning (FIRST)", prune_time],
#     ["baseline duckdb full scan (SECOND)", baseline_time],
#     ["microblock without pruning (LAST)", no_prune_time],
# ]
# print(tabulate(data, headers=["mode", "seconds"], tablefmt="psql"))


import pyarrow as pa
import duckdb
import time
from query_enginev4 import StorageEngine
from tabulate import tabulate

parquet_file = "output.parquet"

# Create persistent connections for BOTH tests
baseline_conn = duckdb.connect()
engine = StorageEngine(parquet_file, table_name="mytable")
query = "select * from mytable where column1 between 18 and 24"

# Warm up both connections equally
print("\n--- warming up both connections ---")
baseline_conn.execute(f"SELECT 1 FROM '{parquet_file}' LIMIT 1").fetchall()
engine.query("select * from mytable limit 1")
print("Warmup complete!")

# NOW run fair tests
print("\n--- baseline duckdb full scan ---")
t1 = time.time()
df_base = baseline_conn.execute(f"""
select * from '{parquet_file}' where column1 between 18 and 24
""").fetchdf()
t2 = time.time()
baseline_time = t2 - t1
print("baseline time:", baseline_time)

print("\n--- microblock engine with pruning ---")
t1 = time.time()
df_mb = engine.query(query)
t2 = time.time()
prune_time = t2 - t1
print("microblock pruning time:", prune_time)

print("\n--- microblock engine no pruning ---")
all_row_groups = list(range(engine.pf.num_row_groups))
t1 = time.time()
tables = [engine.pf.read_row_group(rg) for rg in all_row_groups]
full_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
engine.con.execute("drop table if exists data")
engine.con.register("data", full_table)
df_no_prune = engine.con.execute("select * from data where column1 between 18 and 24").fetchdf()
t2 = time.time()
no_prune_time = t2 - t1
print("microblock no pruning time:", no_prune_time)

# summary
print("\n--- summary ---")
data = [
    ["baseline duckdb (native)", baseline_time],
    ["microblock with pruning (also native)", prune_time],
    ["microblock without pruning (PyArrow)", no_prune_time],
]
print(tabulate(data, headers=["mode", "seconds"], tablefmt="psql"))

baseline_conn.close()
