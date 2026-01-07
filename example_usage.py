from prefetch_scheduler import PrefetchScheduler

def fake_prefetch(block_id):
    print(f"[PREFETCH] Scheduling block {block_id}")

if __name__ == "__main__":
    scheduler = PrefetchScheduler.from_files(
        model_path="lstm_prefetch_model.pt",
        mapping_path="block_id_mappings.json",
        prefetch_threshold=0.6,  # your choice: mode B
        max_history=64,
    )

    query_id = "Q123"

    # Simulate a query reading blocks sequentially + some noise
    access_sequence = [50, 51, 52, 53, 54, 55]

    for b in access_sequence:
        print(f"[ACCESS] Query {query_id} read block {b}")
        scheduler.register_access(query_id, b)

        suggestion = scheduler.suggest_prefetch(query_id)
        if suggestion is not None:
            block_to_prefetch, confidence = suggestion
            print(f" -> Model suggests prefetch {block_to_prefetch} (conf={confidence:.3f})")
            fake_prefetch(block_to_prefetch)
        else:
            print(" -> No prefetch (low confidence or not enough history)")