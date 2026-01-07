import duckdb
import pyarrow.parquet as pq
import re

# class StorageEngine:
#     """
#     Optimized engine that delegates everything to DuckDB's native capabilities.
#     Uses DuckDB's automatic zone map pruning (min-max indexes).
#     """

#     def __init__(self, parquet_path: str, table_name: str = "t1"):
#         self.parquet_path = parquet_path
#         self.table_name = table_name
#         self.con = duckdb.connect()
        
#         # Keep PyArrow ParquetFile for benchmark compatibility
#         self.pf = pq.ParquetFile(parquet_path)

#     def query(self, sql: str):
#         # Rewrite table reference to parquet_scan
#         rewritten_sql = self._rewrite_table_name(
#             sql, 
#             self.table_name, 
#             f"read_parquet('{self.parquet_path}')"
#         )
        
#         # Let DuckDB handle everything with native pruning
#         return self.con.execute(rewritten_sql).df()

#     def _rewrite_table_name(self, sql: str, old: str, new: str) -> str:
#         pattern = r"\bfrom\s+" + re.escape(old) + r"\b"
#         return re.sub(pattern, f"from {new}", sql, flags=re.IGNORECASE)


class StorageEngine:
    """
    Optimized engine that delegates everything to DuckDB's native capabilities.
    Uses DuckDB's automatic zone map pruning (min-max indexes).
    """

    def __init__(
        self,
        parquet_path: str,
        table_name: str = "t1",
        scheduler=None,
        history=None,
        access_logger=None,
        block_cache=None,
    ):
        self.parquet_path = parquet_path
        self.table_name = table_name
        self.scheduler = scheduler
        self.history = history
        self.access_logger = access_logger
        self.block_cache = block_cache

        self.pf = pq.ParquetFile(parquet_path)
        self.num_row_groups = self.pf.num_row_groups
        
        # create the DuckDB view
        self.con = duckdb.connect()
        self.con.execute(f"""
            create or replace view {table_name} as
            select * from read_parquet('{parquet_path}')
        """)

    def _estimate_row_groups(self, sql: str):
        # naive but effective placeholder:
        # select * from parquet_metadata and compare min/max
        meta = duckdb.sql(f"select * from parquet_metadata('{self.parquet_path}')").df()

        # parse predicate using your v3 pruner later
        # for now, simple full scan:
        return list(range(self.num_row_groups))


    def query(self, sql: str):
        # 1. determine which blocks this query may hit
        row_groups = self._estimate_row_groups(sql)

        # 2. update ML history and access logger
        if self.access_logger is not None:
            self.access_logger.log("GLOBAL", row_groups)

        if self.history is not None:
            for rg in row_groups:
                self.history.record(rg)

        if self.scheduler is not None:
            for rg in row_groups:
                self.scheduler.register_access("GLOBAL", rg)

        # 3. check cache and collect cached blocks
        cached_tables = []
        missing = []

        if self.block_cache is not None:
            for rg in row_groups:
                cached = self.block_cache.get(rg)
                if cached is not None:
                    cached_tables.append(cached)
                else:
                    missing.append(rg)
        else:
            missing = row_groups

        # 4. load missing blocks normally with DuckDB (simple path first)
        result = self.con.execute(sql).df()

        return result


    def _rewrite_table_name(self, sql: str, old: str, new: str) -> str:
        pattern = r"\bfrom\s+" + re.escape(old) + r"\b"
        return re.sub(pattern, f"from {new}", sql, flags=re.IGNORECASE)