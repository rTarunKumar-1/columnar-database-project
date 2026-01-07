# prefetch_service.py

import threading
import time
from typing import Optional
from access_logger import GlobalHistory
from prefetch import Prefetcher
from prefetch_scheduler import PrefetchScheduler

class PrefetchService:
    """
    Runs a periodic loop that:
    - looks at recent block access history
    - asks the ML scheduler for next block to prefetch
    - triggers the Prefetcher to load that block into cache
    """

    def __init__(
        self,
        history: GlobalHistory,
        scheduler: PrefetchScheduler,
        prefetcher: Prefetcher,
        interval: int = 60,
        history_len: int = 30,  # Remove min_confidence parameter
    ):
        self.history = history
        self.scheduler = scheduler
        self.prefetcher = prefetcher
        self.interval = interval
        self.history_len = history_len  # Remove self.min_confidence
        self._thread = None
        self._stop_flag = False


    def _run_loop(self):
        """
        Internal loop that runs forever (until stop is called).
        """
        while not self._stop_flag:
            try:
                # get recent sequence from history
                seq = self.history.get_sequence(self.history_len)
                if not seq:
                    print("[PrefetchService] no history yet, skipping this cycle")
                else:
                    print(f"[PrefetchService] got sequence of length {len(seq)} from GlobalHistory")
                    
                    # Get cached block IDs to exclude from predictions
                    cached_blocks = set(self.prefetcher.cache.cache.keys()) if hasattr(self.prefetcher.cache, 'cache') else set()
                    
                    # Get top-10 predictions, excluding already-cached blocks
                    suggestions = self.scheduler.suggest_topk_prefetch(
                        "GLOBAL", 
                        sequence=seq,
                        k=10,
                        exclude_blocks=cached_blocks  # ✅ Use new parameter
                    )
                    
                    if suggestions is None:
                        print("[PrefetchService] scheduler returned no suggestions")
                    else:
                        print(f"[PrefetchService] got {len(suggestions)} suggestions")
                        
                        # Prefetch all suggested blocks
                        prefetched_count = 0
                        for block_id, confidence in suggestions:
                            print(f"[PrefetchService] trying block={block_id}, confidence={confidence:.3f}")
                            prefetched = self.prefetcher.prefetch_block(block_id)
                            if prefetched:
                                prefetched_count += 1
                        
                        print(f"[PrefetchService] prefetched {prefetched_count}/{len(suggestions)} blocks")
                
                time.sleep(self.interval)
                
            except Exception as e:
                print(f"[PrefetchService] error in loop: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(self.interval)



    def start(self):
        if self._thread is not None:
            return
        self._stop_flag = False
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print("[PrefetchService] started periodic predictor")

    def stop(self):
        self._stop_flag = True
        if self._thread:
            self._thread.join(timeout=2.0)
        print("[PrefetchService] stopped")