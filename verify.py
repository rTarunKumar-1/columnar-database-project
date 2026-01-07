# debug_offline_infer.py
import json
import torch
from prefetch_scheduler import PrefetchScheduler

# load scheduler with your trained artifacts
sched = PrefetchScheduler.from_files(
    model_path="trained_model.pt",
    mapping_path="trained_mappings.json",
    prefetch_threshold=0.0
)

events = json.load(open("access_log.json"))
seq = [int(e["block"]) for e in events]
last_seq = seq[-30:]  # take recent history

# feed to scheduler by simulating register_access calls
for b in last_seq:
    sched.register_access("GLOBAL", int(b))

print("history for GLOBAL sample:", sched.query_history.get("GLOBAL")[-30:])

out = sched.suggest_prefetch("GLOBAL")
print("suggestion ->", out)
