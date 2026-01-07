import re
from typing import List

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import sqlglot
from sqlglot import exp

from microblock_index import MicroBlockIndex


class QueryPruner:
    """
    Uses sqlglot plus BlockMetadata statistics to decide which row groups
    may contain qualifying rows for a given sql query.
    """

    def __init__(self, table_id: str, index: MicroBlockIndex):
        self.table_id = table_id
        self.index = index

    def choose_row_groups(self, sql: str, num_row_groups: int) -> List[int]:
        """
        Return list of row_group_ids that may contain qualifying rows.
        If parsing or pruning fails, return all row groups.
        """
        try:
            tree = sqlglot.parse_one(sql)
        except Exception:
            # parsing failed, be conservative
            return list(range(num_row_groups))

        where = tree.find(exp.Where)
        if where is None:
            # no filter, must scan all blocks
            return list(range(num_row_groups))

        condition = where.this

        candidate_groups: List[int] = []
        for rg in range(num_row_groups):
            stats_by_col = self.index.stats_for_row_group(self.table_id, rg)
            if self._expr_may_match(condition, stats_by_col):
                candidate_groups.append(rg)

        return candidate_groups

    def _expr_may_match(self, node, stats_by_col) -> bool:
        """
        Return False if this row group definitely cannot satisfy the predicate.
        Return True if it might match or we are unsure.
        """

        # logical and
        if isinstance(node, exp.And):
            return (
                self._expr_may_match(node.left, stats_by_col)
                and self._expr_may_match(node.right, stats_by_col)
            )

        # logical or
        if isinstance(node, exp.Or):
            return (
                self._expr_may_match(node.left, stats_by_col)
                or self._expr_may_match(node.right, stats_by_col)
            )

        # between: col between low and high
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

            if block_max < low or block_min > high:
                return False
            return True

        # in: col in (v1, v2, v3)
        if isinstance(node, exp.In):
            col = self._column_name(node.this)
            if col is None:
                return True
            stats = stats_by_col.get(col)
            if stats is None or stats.get("min") is None or stats.get("max") is None:
                return True
            block_min = stats["min"]
            block_max = stats["max"]

            values = []
            for e in node.expressions:
                v = self._literal_value(e)
                if v is not None:
                    values.append(v)
            if not values:
                return True

            all_outside = all((v < block_min or v > block_max) for v in values)
            if all_outside:
                return False
            return True

        # basic comparisons: =, !=, <, <=, >, >=
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
            if stats is None or stats.get("min") is None or stats.get("max") is None:
                return True

            block_min = stats["min"]
            block_max = stats["max"]

            if isinstance(node, exp.EQ):
                if const < block_min or const > block_max:
                    return False
                return True

            if isinstance(node, exp.NEQ):
                return True

            if isinstance(node, exp.GT):
                if block_max <= const:
                    return False
                return True

            if isinstance(node, exp.GTE):
                if block_max < const:
                    return False
                return True

            if isinstance(node, exp.LT):
                if block_min >= const:
                    return False
                return True

            if isinstance(node, exp.LTE):
                if block_min > const:
                    return False
                return True

        # unknown expression type, cannot safely prune
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
            return node.this
        return None


class StorageEngine:
    """
    Optimized storage engine that:
    - Uses MicroBlockIndex + QueryPruner to choose row groups
    - Uses PyArrow's read_row_groups() for efficient multi-row-group reading
    - Uses DuckDB's zero-copy Arrow integration
    - Minimizes Python overhead
    """

    def __init__(self, parquet_path: str, table_name: str = "t1"):
        self.parquet_path = parquet_path
        self.table_name = table_name

        # Cache ParquetFile object to avoid repeated file opens
        self.pf = pq.ParquetFile(parquet_path)

        # Build index once from parquet metadata
        self.index = MicroBlockIndex().build_from_parquet(
            parquet_path, table_id=table_name
        )
        self.pruner = QueryPruner(table_name, self.index)

        # DuckDB connection
        self.con = duckdb.connect()

    def query(self, sql: str):
        # 1 - Choose row groups using min/max stats
        row_groups = self.pruner.choose_row_groups(sql, self.pf.num_row_groups)

        # If nothing can possibly match, return empty DataFrame
        if not row_groups:
            return self.con.execute("SELECT 1 WHERE 0").df()

        # Mark access for metadata tracking
        for rg in row_groups:
            for col_name, block in self.index.by_row_group.get(
                (self.table_name, rg), {}
            ).items():
                block.mark_access()

        # 2 - Use PyArrow to read selected row groups efficiently
        if len(row_groups) == self.pf.num_row_groups:
            # If all row groups are selected, read entire file (faster path)
            arrow_table = self.pf.read()
        elif len(row_groups) == 1:
            # Single row group - use read_row_group
            arrow_table = self.pf.read_row_group(row_groups[0], use_threads=True)
        else:
            # Multiple row groups - use read_row_groups for efficient batch read
            arrow_table = self.pf.read_row_groups(row_groups, use_threads=True)

        # 3 - Register Arrow table and query it
        # DuckDB's replacement scan can directly query arrow_table by variable name
        self.con.register("arrow_scan", arrow_table)
        
        # Rewrite SQL to use the registered table
        rewritten_sql = self._rewrite_table_name(sql, self.table_name, "arrow_scan")
        
        # Execute query
        result = self.con.execute(rewritten_sql).df()
        
        # Unregister to free memory
        self.con.unregister("arrow_scan")
        
        return result

    def _rewrite_table_name(self, sql: str, old: str, new: str) -> str:
        """
        Replace table name in SQL query.
        """
        pattern = r"\bfrom\s+" + re.escape(old) + r"\b"
        return re.sub(pattern, f"from {new}", sql, flags=re.IGNORECASE)
