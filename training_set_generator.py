import json

LOG_PATH = "access_log.json"
OUT_DATASET = "training_dataset.json"
OUT_MAPPING = "trained_mappings.json"

def load_block_sequence(path: str = LOG_PATH):
    """Reads access_log.json and returns flat sequence of block ids."""
    try:
        with open(path, "r") as f:
            events = json.load(f)
    except FileNotFoundError:
        print("no access_log.json found - run the interactive engine first")
        return []
    
    seq = []
    for e in events:
        if "block" in e:
            try:
                seq.append(int(e["block"]))
            except Exception:
                continue
    return seq

def build_vocab(block_seq):
    """
    Builds mapping from raw block id to index for embedding.
    Reserve 0 for padding/unknown.
    """
    uniq = sorted(set(block_seq))
    id2idx = {bid: i + 1 for i, bid in enumerate(uniq)}
    idx2id = {i + 1: bid for i, bid in enumerate(uniq)}
    vocab_size = max(idx2id.keys()) + 1  # Plus padding index 0
    return id2idx, idx2id, vocab_size

def build_training_data(block_seq, window=5):
    """Sliding window: input=[seq[i:i+window]], label=seq[i+window]."""
    if len(block_seq) < window + 1:
        return [], []
    
    inputs = []
    labels = []
    for i in range(len(block_seq) - window):
        inputs.append(block_seq[i : i + window])
        labels.append(block_seq[i + window])
    return inputs, labels

def main():
    block_seq = load_block_sequence()
    if not block_seq:
        print("empty access log - generate some queries first")
        return
    
    print(f"loaded {len(block_seq)} block accesses")
    
    id2idx, idx2id, vocab_size = build_vocab(block_seq)
    
    inputs_raw, labels_raw = build_training_data(block_seq, window=5)
    
    # Convert to index space
    inputs_idx = [[id2idx[b] for b in seq] for seq in inputs_raw]
    labels_idx = [id2idx[b] for b in labels_raw]
    
    # ✅ FIX: Save vocab_size in dataset
    dataset = {
        "inputs": inputs_idx,
        "labels": labels_idx,
        "id2idx": id2idx,
        "idx2id": idx2id,
        "vocab_size": vocab_size  # ✅ Added
    }
    
    with open(OUT_DATASET, "w") as f:
        json.dump(dataset, f)
    
    # ✅ FIX: Save vocab_size in mappings too
    mapping = {
        "id2idx": id2idx,
        "idx2id": idx2id,
        "vocab_size": vocab_size  # ✅ Added
    }
    
    with open(OUT_MAPPING, "w") as f:
        json.dump(mapping, f, indent=2)
    
    print(f"Dataset and mappings written. samples: {len(inputs_idx)}, vocab_size: {vocab_size}, window: 5")

if __name__ == "__main__":
    main()
