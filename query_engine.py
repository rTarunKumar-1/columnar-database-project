# import re
# from typing import List, Set

# import duckdb
# import pyarrow.parquet as pq
# import pyarrow as pa
# import sqlglot
# from sqlglot import exp

# from microblock_index import MicroBlockIndex


# class QueryPruner:
#     def __init__(self, table_id: str, index: MicroBlockIndex):
#         self.table_id = table_id
#         self.index = index

#     # high level entry
#     def choose_row_groups(self, sql: str, parquet_path: str) -> List[int]:
#         """
#         return list of row_group_ids that may contain qualifying rows
#         if we cannot understand the where clause, return all row groups
#         """
#         try:
#             tree = sqlglot.parse_one(sql)
#         except Exception:
#             # if parsing fails, do not prune
#             return self._all_row_groups(parquet_path)

#         where = tree.find(exp.Where)
#         if where is None:
#             # no filter at all, cannot prune
#             return self._all_row_groups(parquet_path)

#         condition = where.this  # this is an expression node
#         pf = pq.ParquetFile(parquet_path)

#         candidate_groups: List[int] = []
#         for rg in range(pf.num_row_groups):
#             stats_by_col = self.index.stats_for_row_group(self.table_id, rg)
#             if self._expr_may_match(condition, stats_by_col):
#                 candidate_groups.append(rg)

#         return candidate_groups

#     def _all_row_groups(self, parquet_path: str) -> List[int]:
#         pf = pq.ParquetFile(parquet_path)
#         return list(range(pf.num_row_groups))

#     # core recursive logic
#     def _expr_may_match(self, node, stats_by_col) -> bool:
#         """
#         return False if this row group definitely cannot satisfy the predicate
#         return True if it might match or we are unsure
#         """

#         # logical and
#         if isinstance(node, exp.And):
#             return self._expr_may_match(node.left, stats_by_col) and self._expr_may_match(
#                 node.right, stats_by_col
#             )

#         # logical or
#         if isinstance(node, exp.Or):
#             return self._expr_may_match(node.left, stats_by_col) or self._expr_may_match(
#                 node.right, stats_by_col
#             )

#         # between: col between low and high
#         if isinstance(node, exp.Between):
#             col = self._column_name(node.this)
#             low = self._literal_value(node.args.get("low"))
#             high = self._literal_value(node.args.get("high"))
#             if col is None or low is None or high is None:
#                 return True
#             stats = stats_by_col.get(col)
#             if stats is None or stats.get("min") is None or stats.get("max") is None:
#                 return True

#             block_min = stats["min"]
#             block_max = stats["max"]

#             # if the block range is entirely outside the predicate range, it cannot match
#             if block_max < low or block_min > high:
#                 return False
#             return True

#         # in: col in (v1, v2, v3)
#         if isinstance(node, exp.In):
#             col = self._column_name(node.this)
#             if col is None:
#                 return True
#             stats = stats_by_col.get(col)
#             if stats is None or stats.get("min") is None or stats.get("max") is None:
#                 return True
#             block_min = stats["min"]
#             block_max = stats["max"]

#             values = []
#             for e in node.expressions:
#                 v = self._literal_value(e)
#                 if v is not None:
#                     values.append(v)
#             if not values:
#                 return True

#             # if all values are outside block range, no match
#             all_outside = all((v < block_min or v > block_max) for v in values)
#             if all_outside:
#                 return False
#             return True

#         # basic comparisons: =, !=, <, <=, >, >=
#         if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
#             left_col = self._column_name(node.left)
#             right_col = self._column_name(node.right)
#             left_val = self._literal_value(node.left)
#             right_val = self._literal_value(node.right)

#             # we only handle col op constant or constant op col
#             col = None
#             const = None
#             if left_col is not None and right_val is not None:
#                 col = left_col
#                 const = right_val
#             elif right_col is not None and left_val is not None:
#                 col = right_col
#                 const = left_val
#             else:
#                 return True

#             stats = stats_by_col.get(col)
#             if stats is None or stats.get("min") is None or stats.get("max") is None:
#                 return True

#             block_min = stats["min"]
#             block_max = stats["max"]

#             # choose rule by operator type
#             if isinstance(node, exp.EQ):
#                 # if constant is outside block range, no match
#                 if const < block_min or const > block_max:
#                     return False
#                 return True

#             if isinstance(node, exp.NEQ):
#                 # almost impossible to rule out by min max alone
#                 return True

#             if isinstance(node, exp.GT):
#                 # col > const
#                 if block_max <= const:
#                     return False
#                 return True

#             if isinstance(node, exp.GTE):
#                 # col >= const
#                 if block_max < const:
#                     return False
#                 return True

#             if isinstance(node, exp.LT):
#                 # col < const
#                 if block_min >= const:
#                     return False
#                 return True

#             if isinstance(node, exp.LTE):
#                 # col <= const
#                 if block_min > const:
#                     return False
#                 return True

#         # any expression we do not understand we treat as "maybe"
#         return True

#     def _column_name(self, node):
#         if isinstance(node, exp.Column):
#             return node.name
#         return None

#     def _literal_value(self, node):
#         if isinstance(node, exp.Literal):
#             # sqlglot gives .this as string, cast based on is_int or is_number etc
#             if node.is_int:
#                 return int(node.this)
#             if node.is_number:
#                 return float(node.this)
#             return node.this  # string literal
#         return None


# class StorageEngine:
#     def __init__(self, parquet_path: str, table_name: str = "t1"):
#         self.parquet_path = parquet_path
#         self.table_name = table_name

#         # build index once
#         self.index = MicroBlockIndex().build_from_parquet(parquet_path, table_id=table_name)
#         self.pruner = QueryPruner(table_name, self.index)

#         # parquet file handle
#         self.pf = pq.ParquetFile(parquet_path)

#         # duckdb connection
#         self.con = duckdb.connect()

#     def query(self, sql: str):
#         row_groups = self.pruner.choose_row_groups(sql, self.parquet_path)

#         tables = []
#         for rg in row_groups:
#             t = self.pf.read_row_group(rg)
#             tables.append(t)
#             for col_name, block in self.index.by_row_group[(self.table_name, rg)].items():
#                 block.mark_access()

#         if not tables:
#             return self.con.query("select 1 where 0").df()

#         full_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

#         # check existing registered tables properly
#         existing_tables = {
#             row[0] 
#             for row in self.con.execute("PRAGMA show_tables").fetchall()
#         }

#         if "data" in existing_tables:
#             self.con.unregister("data")

#         self.con.register("data", full_table)

#         rewritten_sql = self._rewrite_sql_to_data(sql)
#         return self.con.execute(rewritten_sql).df()


#     def _rewrite_sql_to_data(self, sql: str) -> str:
#         """
#         very simple rewrite: replace "from t1" with "from data"
#         you can make this smarter if needed
#         """
#         pattern = r"FROM\s+" + re.escape(self.table_name) + r"\b"
#         return re.sub(pattern, "FROM data", sql, flags=re.IGNORECASE)
