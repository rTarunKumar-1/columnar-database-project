# query_engine_v5.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from typing import List

import sqlglot
from sqlglot import exp

from microblock_index import MicroBlockIndex
from access_logger import AccessLogger, GlobalHistory
from block_cache import BlockCache
from prefetch_scheduler import PrefetchScheduler


class StorageEngineV5:
    """
    Cache aware microblock storage engine.

    This engine does:
      - builds MicroBlockIndex on the Parquet file
      - uses sqlglot plus column min max stats to prune row groups
      - uses BlockCache to reuse prefetched microblocks
      - logs access and updates GlobalHistory and PrefetchScheduler
      - runs queries inside DuckDB on a merged Arrow table
    """

    def __init__(
        self,
        parquet_path: str,
        table_name: str = "t1",
        scheduler: PrefetchScheduler | None = None,
        history: GlobalHistory | None = None,
        access_logger: AccessLogger | None = None,
        block_cache: BlockCache | None = None,
    ):
        self.parquet_path = parquet_path
        self.table_name = table_name

        self.scheduler = scheduler
        self.history = history
        self.access_logger = access_logger
        self.block_cache = block_cache

        self.pf = pq.ParquetFile(parquet_path)
        self.num_row_groups = self.pf.num_row_groups

        self.mb_index = MicroBlockIndex().build_from_parquet(
            parquet_path, table_id=table_name
        )

        self.con = duckdb.connect()
        self.con.execute(f"""
            create or replace view {table_name} as
            select * from read_parquet('{parquet_path}')
        """)

    # ------------------------------------------------------------
    # pruning using MicroBlockIndex stats and sqlglot
    # ------------------------------------------------------------

    def _estimate_row_groups(self, sql: str) -> List[int]:
        """
        Use MicroBlockIndex plus min max stats to prune row groups.

        Steps:
          - parse SQL with sqlglot
          - extract WHERE expression
          - for each row group, get stats_for_row_group
          - call _expr_may_match to see if the group can satisfy predicate
        """
        try:
            tree = sqlglot.parse_one(sql)
        except Exception:
            # cannot parse, scan all
            return list(range(self.num_row_groups))

        where = tree.find(exp.Where)
        if where is None:
            # no filter, scan all
            return list(range(self.num_row_groups))

        condition = where.this

        candidate_groups: List[int] = []
        for rg in range(self.num_row_groups):
            stats_by_col = self.mb_index.stats_for_row_group(self.table_name, rg)
            if self._expr_may_match(condition, stats_by_col):
                candidate_groups.append(rg)

        if not candidate_groups:
            # always keep at least something to avoid weird corner cases
            return list(range(self.num_row_groups))

        return candidate_groups

    def _expr_may_match(self, node, stats_by_col) -> bool:
        """
        Return False if this row group definitely cannot satisfy predicate.
        Return True if it might match or we cannot be sure.

        Uses only column level min max, so this is conservative.
        """

        # and
        if isinstance(node, exp.And):
            return (
                self._expr_may_match(node.left, stats_by_col)
                and self._expr_may_match(node.right, stats_by_col)
            )

        # or
        if isinstance(node, exp.Or):
            return (
                self._expr_may_match(node.left, stats_by_col)
                or self._expr_may_match(node.right, stats_by_col)
            )

        # between
        if isinstance(node, exp.Between):
            col = self._column_name(node.this)
            low = self._literal_value(node.args.get("low"))
            high = self._literal_value(node.args.get("high"))
            if col is None or low is None or high is None:
                return True
            stats = stats_by_col.get(col)
            if stats is None or stats.get("min") is None or stats.get("max") is None:
                return True
            block_min = stats["min"]
            block_max = stats["max"]
            # no overlap with predicate range
            if block_max < low or block_min > high:
                return False
            return True

        # in operator
        if isinstance(node, exp.In):
            col = self._column_name(node.this)
            if col is None:
                return True
            stats = stats_by_col.get(col)
            if stats is None:
                return True
            block_min = stats.get("min")
            block_max = stats.get("max")
            if block_min is None or block_max is None:
                return True

            values = []
            for e in node.expressions:
                v = self._literal_value(e)
                if v is not None:
                    values.append(v)

            if not values:
                return True

            # if all values are outside block range, cannot match
            outside = all(v < block_min or v > block_max for v in values)
            if outside:
                return False
            return True

        # simple comparisons
        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            left_col = self._column_name(node.left)
            right_col = self._column_name(node.right)
            left_val = self._literal_value(node.left)
            right_val = self._literal_value(node.right)

            col = None
            const = None

            if left_col is not None and right_val is not None:
                col = left_col
                const = right_val
            elif right_col is not None and left_val is not None:
                col = right_col
                const = left_val
            else:
                return True

            stats = stats_by_col.get(col)
            if stats is None:
                return True
            block_min = stats.get("min")
            block_max = stats.get("max")
            if block_min is None or block_max is None:
                return True

            if isinstance(node, exp.EQ):
                return block_min <= const <= block_max

            if isinstance(node, exp.NEQ):
                # cannot rule out with min max easily
                return True

            if isinstance(node, exp.GT):
                # col > const
                return block_max > const

            if isinstance(node, exp.GTE):
                return block_max >= const

            if isinstance(node, exp.LT):
                # col < const
                return block_min < const

            if isinstance(node, exp.LTE):
                return block_min <= const

        # unknown node types, be conservative
        return True

    def _column_name(self, node):
        if isinstance(node, exp.Column):
            return node.name
        return None

    def _literal_value(self, node):
        if isinstance(node, exp.Literal):
            if node.is_int:
                return int(node.this)
            if node.is_number:
                return float(node.this)
            # treat as string for now
            return node.this
        return None

    # ------------------------------------------------------------
    # concat helper
    # ------------------------------------------------------------
    def _concat_tables(self, tables: List[pa.Table]) -> pa.Table:
        if len(tables) == 1:
            return tables[0]
        return pa.concat_tables(tables, promote=True)
    # ------------------------------------------------------------
    # main query with cache integration
    # ------------------------------------------------------------
    def query(self, sql: str):
        row_groups = self._estimate_row_groups(sql)
        print(f"[Engine] candidate row groups for this query: {row_groups}")
        self.last_row_groups = row_groups


        # if self.access_logger:
        #     self.access_logger.log("GLOBAL", row_groups)

        if self.access_logger:
            self.access_logger.log(row_groups)


        if self.history:
            for rg in row_groups:
                self.history.record(rg)

        if self.scheduler:
            for rg in row_groups:
                self.scheduler.register_access("GLOBAL", rg)

        cached_tables = []
        missing = []

        if self.block_cache:
            for rg in row_groups:
                tbl = self.block_cache.get(rg)
                if tbl is not None:
                    print(f"[Engine] cache hit on block {rg}")
                    cached_tables.append(tbl)
                else:
                    print(f"[Engine] cache miss on block {rg}")
                    missing.append(rg)
        else:
            missing = row_groups

        missing_tables = []
        for rg in missing:
            t = self.pf.read_row_group(rg)
            print(f"[Engine] loaded block {rg} from Parquet")
            missing_tables.append(t)

        all_tables = cached_tables + missing_tables

        if not all_tables:
            print("[Engine] no tables to query, returning empty result via DuckDB fallback")
            return self.con.execute(sql).df()

        merged = self._concat_tables(all_tables)

        # defensive unregister
        try:
            tables_df = self.con.execute("pragma show_tables").fetchdf()
            if "microblock_data" in tables_df["name"].tolist():
                self.con.unregister("microblock_data")
        except Exception:
            pass

        self.con.register("microblock_data", merged)

        rewritten_sql = sql.replace(self.table_name, "microblock_data")
        print(f"[Engine] executing rewritten sql: {rewritten_sql}")

        return self.con.execute(rewritten_sql).df()