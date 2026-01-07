###  code checks if row_groups is compatible or not. If its compatible it uses it, if not uses pyarrow itself ###

import re
import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import sqlglot
from sqlglot import exp

from microblock_index import MicroBlockIndex


class QueryPruner:
    def __init__(self, table_id, index):
        self.table_id = table_id
        self.index = index

    def choose_row_groups(self, sql, parquet_path):
        try:
            tree = sqlglot.parse_one(sql)
        except Exception:
            return self._all(parquet_path)

        where = tree.find(exp.Where)
        if where is None:
            return self._all(parquet_path)

        cond = where.this
        pf = pq.ParquetFile(parquet_path)

        out = []
        for rg in range(pf.num_row_groups):
            stats = self.index.stats_for_row_group(self.table_id, rg)
            if self._may_match(cond, stats):
                out.append(rg)
        return out

    def _all(self, parquet_path):
        pf = pq.ParquetFile(parquet_path)
        return list(range(pf.num_row_groups))

    def _may_match(self, node, stats):
        if isinstance(node, exp.And):
            return self._may_match(node.left, stats) and self._may_match(node.right, stats)

        if isinstance(node, exp.Or):
            return self._may_match(node.left, stats) or self._may_match(node.right, stats)

        if isinstance(node, exp.Between):
            col = self._col(node.this)
            low = self._lit(node.args.get("low"))
            high = self._lit(node.args.get("high"))
            if col is None or low is None or high is None:
                return True
            s = stats.get(col)
            if s is None or s.get("min") is None or s.get("max") is None:
                return True
            if s["max"] < low or s["min"] > high:
                return False
            return True

        if isinstance(node, exp.In):
            col = self._col(node.this)
            if col is None:
                return True
            s = stats.get(col)
            if s is None:
                return True
            lo = s.get("min")
            hi = s.get("max")
            if lo is None or hi is None:
                return True
            vals = []
            for e in node.expressions:
                v = self._lit(e)
                if v is not None:
                    vals.append(v)
            if not vals:
                return True
            outside = all(v < lo or v > hi for v in vals)
            return not outside

        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            lc = self._col(node.left)
            rc = self._col(node.right)
            lv = self._lit(node.left)
            rv = self._lit(node.right)

            col = None
            const = None
            if lc is not None and rv is not None:
                col = lc
                const = rv
            elif rc is not None and lv is not None:
                col = rc
                const = lv
            else:
                return True

            s = stats.get(col)
            if s is None:
                return True
            lo = s.get("min")
            hi = s.get("max")
            if lo is None or hi is None:
                return True

            if isinstance(node, exp.EQ):
                return lo <= const <= hi
            if isinstance(node, exp.GT):
                return hi > const
            if isinstance(node, exp.GTE):
                return hi >= const
            if isinstance(node, exp.LT):
                return lo < const
            if isinstance(node, exp.LTE):
                return lo <= const
            if isinstance(node, exp.NEQ):
                return True

        return True

    def _col(self, node):
        return node.name if isinstance(node, exp.Column) else None

    def _lit(self, node):
        if isinstance(node, exp.Literal):
            if node.is_int:
                return int(node.this)
            if node.is_number:
                return float(node.this)
            return node.this
        return None


class StorageEngine:
    def __init__(self, parquet_path, table_name="t1"):
        self.parquet_path = parquet_path
        self.table_name = table_name

        self.index = MicroBlockIndex().build_from_parquet(parquet_path, table_name)
        self.pruner = QueryPruner(table_name, self.index)
        self.pf = pq.ParquetFile(parquet_path)
        self.con = duckdb.connect()

        # detect row_group support once
        self.has_row_group = self._detect_row_group_support()

    def _detect_row_group_support(self):
        try:
            self.con.execute(
                f"select * from read_parquet('{self.parquet_path}', row_group=0)"
            )
            return True
        except:
            return False

    def query(self, sql):
        row_groups = self.pruner.choose_row_groups(sql, self.parquet_path)

        if not row_groups:
            return self.con.execute("select 1 where 0").df()

        for rg in row_groups:
            for _, b in self.index.by_row_group[(self.table_name, rg)].items():
                b.mark_access()

        if self.has_row_group:
            parts = [
                f"select * from read_parquet('{self.parquet_path}', row_group={rg})"
                for rg in row_groups
            ]
            union_sql = " union all ".join(parts)
            rewritten = self._rewrite(sql, self.table_name, "filtered")
            final = f"with filtered as ({union_sql}) {rewritten}"
            return self.con.execute(final).df()

        arrow_tables = []
        for rg in row_groups:
            arrow_tables.append(self.pf.read_row_group(rg))
        combined = pa.concat_tables(arrow_tables)

        self.con.execute("drop table if exists filtered")
        self.con.register("filtered", combined)

        rewritten = self._rewrite(sql, self.table_name, "filtered")
        return self.con.execute(rewritten).df()

    def _rewrite(self, sql, old, new):
        pat = r"\bfrom\s+" + re.escape(old) + r"\b"
        return re.sub(pat, f"from {new}", sql, flags=re.IGNORECASE)
