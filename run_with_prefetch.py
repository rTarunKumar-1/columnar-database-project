# run_with_prefetch.py

from block_cache import BlockCache
from access_logger import AccessLogger, GlobalHistory
from prefetch_scheduler import PrefetchScheduler
from prefetch import Prefetcher
from prefetch_service import PrefetchService
from query_enginev5 import StorageEngineV5


PARQUET_PATH = "output_microblocks.parquet"
TABLE_NAME = "mytable"

# core shared components
cache = BlockCache(capacity=64)
history = GlobalHistory(maxlen=200)
logger = AccessLogger()

# scheduler = PrefetchScheduler(
#     model_path="model.pth",
#     block_vocab_path="block_vocab.json"
# )

scheduler = PrefetchScheduler.from_files(
    model_path="lstm_prefetch_model.pt",
    mapping_path="block_id_mappings.json",   # this is how your file is named
    prefetch_threshold=0.6,
    max_history=64
)

prefetcher = Prefetcher(PARQUET_PATH, cache)

# start periodic ML prefetch loop
service = PrefetchService(
    history=history,
    scheduler=scheduler,
    prefetcher=prefetcher,
    interval=60,
    min_confidence=0.4,
    history_len=30,
)
service.start()

# build engine with all hooks
engine = StorageEngineV5(
    parquet_path=PARQUET_PATH,
    table_name=TABLE_NAME,
    scheduler=scheduler,
    history=history,
    access_logger=logger,
    block_cache=cache,
)

# example query usage
if __name__ == "__main__":
    sql = "select * from mytable where column1 between 18 and 24"
    df = engine.query(sql)
    print(df.head())
