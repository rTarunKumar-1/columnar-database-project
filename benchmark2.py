# #### 122k first then 16k. first baseline then microblock with pruning ### 

import pyarrow as pa
import duckdb
import time
from query_enginev4 import StorageEngine
from tabulate import tabulate

# Test both files
files_to_test = [
    ("output_microblocks.parquet", "16k row groups"),
    ("output.parquet", "122k row groups")
]

# Use the SAME query for all tests
# query_filter = "select * from mytable where column1 between 18 and 24"
query_filter = "select * from mytable"

all_results = []

for parquet_file, description in files_to_test:
    print(f"\n{'='*60}")
    print(f"Testing: {parquet_file} ({description})")
    print(f"{'='*60}")
    
    # Create persistent connections for BOTH tests
    baseline_conn = duckdb.connect()
    engine = StorageEngine(parquet_file, table_name="mytable")
    
    # Warm up both connections equally
    print("\n--- warming up both connections ---")
    baseline_conn.execute(f"SELECT 1 FROM '{parquet_file}' LIMIT 1").fetchall()
    engine.query("select * from mytable limit 1")
    print(f"Warmup complete! File has {engine.pf.num_row_groups} row groups")
    
    # Test 1: Baseline DuckDB (using filter query)
    print("\n--- baseline duckdb full scan ---")
    t1 = time.time()
    df_base = baseline_conn.execute(f"""
    select * from '{parquet_file}' where column1 between 18 and 24
    """).fetchdf()
    t2 = time.time()
    baseline_time = t2 - t1
    print(f"baseline time: {baseline_time:.4f}s")
    
    # Test 2: Microblock with pruning (using SAME filter query)
    print("\n--- microblock engine with pruning ---")
    t1 = time.time()
    df_mb = engine.query(query_filter)  # Now uses the filter!
    t2 = time.time()
    prune_time = t2 - t1
    print(f"microblock pruning time: {prune_time:.4f}s")
    
    # Test 3: Microblock without pruning (PyArrow)
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
    print(f"microblock no pruning time: {no_prune_time:.4f}s")
    
    # Store results
    all_results.append({
        'file': description,
        'baseline': baseline_time,
        'pruning': prune_time,
        'no_pruning': no_prune_time,
        'row_groups': engine.pf.num_row_groups
    })
    
    # Summary for this file
    print(f"\n--- summary for {description} ---")
    data = [
        ["baseline duckdb (native)", baseline_time],
        ["microblock with pruning (also native)", prune_time],
        ["microblock without pruning (PyArrow)", no_prune_time],
    ]
    print(tabulate(data, headers=["mode", "seconds"], tablefmt="psql"))
    
    baseline_conn.close()

# Final comparison
print(f"\n{'='*60}")
print("FINAL COMPARISON")
print(f"{'='*60}")

comparison_data = []
for result in all_results:
    comparison_data.append([
        f"{result['file']} - Baseline",
        result['row_groups'],
        f"{result['baseline']:.4f}s"
    ])
    comparison_data.append([
        f"{result['file']} - Pruning",
        result['row_groups'],
        f"{result['pruning']:.4f}s"
    ])
    comparison_data.append([
        f"{result['file']} - No Pruning (PyArrow)",
        result['row_groups'],
        f"{result['no_pruning']:.4f}s"
    ])
    comparison_data.append(["---", "---", "---"])

print(tabulate(comparison_data, headers=["Test", "Row Groups", "Time"], tablefmt="psql"))

# Key insights
print("\n--- KEY INSIGHTS ---")
print(f"122k row groups baseline: {all_results[0]['baseline']:.4f}s")
print(f"16k row groups baseline:  {all_results[1]['baseline']:.4f}s")
print(f"Difference: {abs(all_results[0]['baseline'] - all_results[1]['baseline']):.4f}s")
if all_results[1]['baseline'] < all_results[0]['baseline']:
    print(f"16k is {all_results[0]['baseline']/all_results[1]['baseline']:.2f}x FASTER than 122k")
else:
    print(f"122k is {all_results[1]['baseline']/all_results[0]['baseline']:.2f}x FASTER than 16k")



# import pyarrow as pa
# import duckdb
# import time
# from query_enginev4 import StorageEngine
# from tabulate import tabulate

# # Test both files - 16k FIRST, then 122k
# files_to_test = [
#     ("output_microblocks.parquet", "16k row groups"),
#     ("output.parquet", "122k row groups")
# ]

# query_filter = "select * from mytable where column1 between 18 and 24"

# all_results = []

# for parquet_file, description in files_to_test:
#     print(f"\n{'='*60}")
#     print(f"Testing: {parquet_file} ({description})")
#     print(f"{'='*60}")
    
#     # Create persistent connections for BOTH tests
#     baseline_conn = duckdb.connect()
#     engine = StorageEngine(parquet_file, table_name="mytable")
    
#     # Warm up both connections equally
#     print("\n--- warming up both connections ---")
#     baseline_conn.execute(f"SELECT 1 FROM '{parquet_file}' LIMIT 1").fetchall()
#     engine.query("select * from mytable limit 1")
#     print(f"Warmup complete! File has {engine.pf.num_row_groups} row groups")
    
#     # Test 1: Baseline DuckDB
#     print("\n--- baseline duckdb full scan ---")
#     t1 = time.time()
#     df_base = baseline_conn.execute(f"""
#     select * from '{parquet_file}' where column1 between 18 and 24
#     """).fetchdf()
#     t2 = time.time()
#     baseline_time = t2 - t1
#     print(f"baseline time: {baseline_time:.4f}s")
    
#     # Test 2: Microblock with pruning
#     print("\n--- microblock engine with pruning ---")
#     t1 = time.time()
#     df_mb = engine.query(query_filter)
#     t2 = time.time()
#     prune_time = t2 - t1
#     print(f"microblock pruning time: {prune_time:.4f}s")
    
#     # Test 3: Microblock without pruning (PyArrow)
#     print("\n--- microblock engine no pruning ---")
#     all_row_groups = list(range(engine.pf.num_row_groups))
#     t1 = time.time()
#     tables = [engine.pf.read_row_group(rg) for rg in all_row_groups]
#     full_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
#     engine.con.execute("drop table if exists data")
#     engine.con.register("data", full_table)
#     df_no_prune = engine.con.execute("select * from data where column1 between 18 and 24").fetchdf()
#     t2 = time.time()
#     no_prune_time = t2 - t1
#     print(f"microblock no pruning time: {no_prune_time:.4f}s")
    
#     # Store results
#     all_results.append({
#         'file': description,
#         'baseline': baseline_time,
#         'pruning': prune_time,
#         'no_pruning': no_prune_time,
#         'row_groups': engine.pf.num_row_groups
#     })
    
#     # Summary for this file
#     print(f"\n--- summary for {description} ---")
#     data = [
#         ["baseline duckdb (native)", baseline_time],
#         ["microblock with pruning (also native)", prune_time],
#         ["microblock without pruning (PyArrow)", no_prune_time],
#     ]
#     print(tabulate(data, headers=["mode", "seconds"], tablefmt="psql"))
    
#     baseline_conn.close()

# # Final comparison
# print(f"\n{'='*60}")
# print("FINAL COMPARISON")
# print(f"{'='*60}")

# comparison_data = []
# for result in all_results:
#     comparison_data.append([
#         f"{result['file']} - Baseline",
#         result['row_groups'],
#         f"{result['baseline']:.4f}s"
#     ])
#     comparison_data.append([
#         f"{result['file']} - Pruning",
#         result['row_groups'],
#         f"{result['pruning']:.4f}s"
#     ])
#     comparison_data.append([
#         f"{result['file']} - No Pruning (PyArrow)",
#         result['row_groups'],
#         f"{result['no_pruning']:.4f}s"
#     ])
#     comparison_data.append(["---", "---", "---"])

# print(tabulate(comparison_data, headers=["Test", "Row Groups", "Time"], tablefmt="psql"))

# # Key insights
# print("\n--- KEY INSIGHTS ---")
# print(f"16k row groups baseline:  {all_results[0]['baseline']:.4f}s (FIRST RUN)")
# print(f"122k row groups baseline: {all_results[1]['baseline']:.4f}s (SECOND RUN)")
# print(f"Difference: {abs(all_results[1]['baseline'] - all_results[0]['baseline']):.4f}s")
# if all_results[0]['baseline'] > all_results[1]['baseline']:
#     print(f"16k is {all_results[0]['baseline']/all_results[1]['baseline']:.2f}x SLOWER than 122k")
# else:
#     print(f"16k is {all_results[1]['baseline']/all_results[0]['baseline']:.2f}x FASTER than 122k")
# print(f"\nOptimal row group size: 100k-1M rows (your 122k is near optimal)")
# print(f"16k row groups have overhead from: more metadata, more boundaries, less compression")
