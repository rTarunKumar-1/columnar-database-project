import time
from block_cache import BlockCache
from access_logger import AccessLogger, GlobalHistory
from prefetch_scheduler import PrefetchScheduler
from prefetch import Prefetcher
from prefetch_service import PrefetchService
from query_enginev5 import StorageEngineV5

PARQUET_PATH = "output_microblocks.parquet"
TABLE_NAME = "mytable"

cache = BlockCache(capacity=128)
history = GlobalHistory(maxlen=500)
logger = AccessLogger(path="access_log.json")

print("\n=== Loading Model and Scheduler ===")
scheduler = PrefetchScheduler.from_files(
    model_path="trained_model.pt",
    mapping_path="trained_mappings.json",
    prefetch_threshold=0.4,  
    max_history=64
)

prefetcher = Prefetcher(PARQUET_PATH, cache)

service = PrefetchService(
    history=history,
    scheduler=scheduler,
    prefetcher=prefetcher,
    interval=60,  
    # min_confidence=0.3,  
    history_len=100
)

service.start()

engine = StorageEngineV5(
    parquet_path=PARQUET_PATH,
    table_name=TABLE_NAME,
    scheduler=scheduler,
    history=history,
    access_logger=logger,
    block_cache=cache
)

print(f"""
=== Microblock Engine Interactive Shell ===
Type SQL queries using '{TABLE_NAME}'.
Example: select * from {TABLE_NAME} where column1 between 18 and 24;
Press CTRL+C to exit.
Prefetcher is running every 60 seconds in background.
""")

try:
    while True:
        raw = input("SQL> ")
        if not raw.strip():
            continue

        result = engine.query(raw)
        print(result)

except (KeyboardInterrupt, EOFError):
    print("\nExiting interactive shell...")
finally:
    service.stop()