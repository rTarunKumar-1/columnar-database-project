import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from torch.nn.utils.rnn import pad_sequence, pack_padded_sequence, pad_packed_sequence

import os
import json


############################################
# 1 Synthetic Mixed-Pattern Log Generator
############################################

def generate_mixed_logs(
    num_queries=300,
    base_patterns=[(0, 25), (50, 80), (100, 135)],
    noise_prob=0.10,
    long_jump_prob=0.05,
    save_path="mixed_access_log.csv"
):
    """
    Mixed workload:
    - Mostly sequential within a chosen range (pattern)
    - Sometimes short jumps within range
    - Sometimes long random jumps (to simulate noise)
    """
    rows = []
    current_time = datetime.now()

    for qid in range(num_queries):
        # Pick a base pattern (range of blocks) for this query
        start, end = random.choice(base_patterns)
        length = end - start + 1

        # Decide direction (forward or backward scan)
        direction = random.choice([1, 1, 1, -1])  # mostly forward
        if direction == 1:
            blocks = list(range(start, end + 1))
        else:
            blocks = list(range(end, start - 1, -1))

        # Walk through this range, but introduce noise/jumps
        idx = 0
        while 0 <= idx < len(blocks):
            base_block = blocks[idx]

            r = random.random()
            if r < long_jump_prob:
                # Long random jump to anywhere in global space
                block = random.randint(0, max(b[1] for b in base_patterns))
            elif r < long_jump_prob + noise_prob:
                # Short random jump within the same pattern range
                block = random.randint(start, end)
            else:
                # Normal sequential access
                block = base_block

            rows.append({
                "time": current_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "query_id": f"Q{qid}",
                "block_id": block
            })
            # Advance time a bit
            current_time += timedelta(milliseconds=random.randint(5, 20))

            # Move index
            step = random.choice([1, 1, 1, 2])  # mostly step of 1
            idx += step * direction

    df = pd.DataFrame(rows)
    df.to_csv(save_path, index=False)
    print(f"Generated mixed dataset: {save_path}")
    return df


############################################
# 2️ Build Sequence Dataset (Dynamic Length)
############################################

def build_sequences_from_log(log_path):
    """
    From the log:
    - Group by query_id
    - For each query, sort by time, take block sequence
    - For each prefix of the sequence, create (seq_prefix, next_block)
      so we get dynamic-length sequences.
    """
    df = pd.read_csv(log_path)
    df = df.sort_values(by="time")

    sequences = []
    labels = []

    # Group by query
    for qid, group in df.groupby("query_id"):
        blocks = group["block_id"].tolist()
        if len(blocks) < 2:
            continue

        # For dynamic sequences: prefix of length 1..(n-1)
        for i in range(1, len(blocks)):
            seq = blocks[:i]        # prefix
            nxt = blocks[i]         # next block
            sequences.append(seq)
            labels.append(nxt)

    print(f"Total training examples: {len(sequences)}")

    # Map block_ids to contiguous indices for embedding
    all_ids = sorted(set([b for seq in sequences for b in seq] + labels))
    id2idx = {bid: i + 1 for i, bid in enumerate(all_ids)}  # +1 to reserve 0 for padding
    idx2id = {i + 1: bid for i, bid in enumerate(all_ids)}

    # Convert sequences & labels to index space
    seq_idx = [[id2idx[b] for b in seq] for seq in sequences]
    labels_idx = [id2idx[b] for b in labels]

    return seq_idx, labels_idx, id2idx, idx2id


############################################
# 3️ PyTorch Dataset & Collate Function
############################################

class BlockSeqDataset(Dataset):
    def __init__(self, seqs, labels):
        self.seqs = seqs
        self.labels = labels

    def __len__(self):
        return len(self.seqs)

    def __getitem__(self, idx):
        seq = torch.tensor(self.seqs[idx], dtype=torch.long)
        label = torch.tensor(self.labels[idx], dtype=torch.long)
        length = len(self.seqs[idx])
        return seq, length, label


def collate_fn(batch):
    """
    Pads sequences to same length in a batch.
    Returns:
      padded_seqs: [B, T]
      lengths: [B]
      labels: [B]
    """
    seqs, lengths, labels = zip(*batch)
    lengths = torch.tensor(lengths, dtype=torch.long)
    labels = torch.tensor(labels, dtype=torch.long)
    padded_seqs = pad_sequence(seqs, batch_first=True, padding_value=0)  # 0 = PAD
    return padded_seqs, lengths, labels


############################################
# 4️ LSTM Model
############################################

class LSTMPrefetcher(nn.Module):
    def __init__(self, num_tokens, embed_dim=16, hidden_dim=64, num_layers=1):
        """
        num_tokens: vocabulary size (max index + 1)
        """
        super().__init__()
        self.embedding = nn.Embedding(num_tokens, embed_dim, padding_idx=0)
        self.lstm = nn.LSTM(
            input_size=embed_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True
        )
        self.fc = nn.Linear(hidden_dim, num_tokens)  # classification over block indices

    def forward(self, seqs, lengths):
        """
        seqs: [B, T] Long
        lengths: [B] Long
        """
        embedded = self.embedding(seqs)   # [B, T, E]

        # Pack for dynamic lengths
        packed = pack_padded_sequence(
            embedded, lengths.cpu(), batch_first=True, enforce_sorted=False
        )
        packed_out, (h_n, c_n) = self.lstm(packed)

        # Use last hidden state from top layer: [B, H]
        last_hidden = h_n[-1]

        logits = self.fc(last_hidden)  # [B, num_tokens]
        return logits


############################################
# 5️ Training & Evaluation
############################################

def train_model(
    model,
    train_loader,
    val_loader,
    num_epochs=10,
    lr=1e-3,
    device="cpu"
):
    model.to(device)
    criterion = nn.CrossEntropyLoss()  # labels are indices
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    for epoch in range(1, num_epochs + 1):
        model.train()
        total_loss = 0.0
        total_samples = 0

        for seqs, lengths, labels in train_loader:
            seqs = seqs.to(device)
            lengths = lengths.to(device)
            labels = labels.to(device)

            optimizer.zero_grad()
            logits = model(seqs, lengths)
            loss = criterion(logits, labels)
            loss.backward()
            optimizer.step()

            batch_size = labels.size(0)
            total_loss += loss.item() * batch_size
            total_samples += batch_size

        avg_loss = total_loss / total_samples if total_samples > 0 else 0.0

        # Simple validation accuracy
        val_acc = evaluate_accuracy(model, val_loader, device=device)
        print(f"Epoch {epoch:02d} | Train Loss: {avg_loss:.4f} | Val Acc: {val_acc:.4f}")

    return model


def evaluate_accuracy(model, loader, device="cpu"):
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for seqs, lengths, labels in loader:
            seqs = seqs.to(device)
            lengths = lengths.to(device)
            labels = labels.to(device)

            logits = model(seqs, lengths)
            preds = torch.argmax(logits, dim=1)
            correct += (preds == labels).sum().item()
            total += labels.size(0)
    return correct / total if total > 0 else 0.0


def evaluate_prefetch_hit_rate(model, loader, device="cpu"):
    """
    Prefetch hit-rate = fraction of times predicted block == actual next block.
    Same as accuracy for this next-block task.
    """
    return evaluate_accuracy(model, loader, device=device)


############################################
# 6️ Main Pipeline
############################################

if __name__ == "__main__":
    # ---- Step 1: Generate mixed synthetic log ----
    log_path = "mixed_access_log.csv"
    if not os.path.exists(log_path):
        generate_mixed_logs(save_path=log_path)
    else:
        print(f"Reusing existing log: {log_path}")

    # ---- Step 2: Build dynamic sequences ----
    seqs, labels, id2idx, idx2id = build_sequences_from_log(log_path)
    num_tokens = max(idx2id.keys()) + 1  # +1 because indices start at 1, 0 is PAD

    # Save mappings for later use (in the scheduler)
    with open("block_id_mappings.json", "w") as f:
        json.dump({"id2idx": id2idx, "idx2id": idx2id}, f)
    print("Saved block_id_mappings.json")

    # ---- Step 3: Train/val split ----
    N = len(seqs)
    indices = list(range(N))
    random.shuffle(indices)

    split = int(0.8 * N)
    train_idx, val_idx = indices[:split], indices[split:]

    train_seqs = [seqs[i] for i in train_idx]
    train_labels = [labels[i] for i in train_idx]
    val_seqs = [seqs[i] for i in val_idx]
    val_labels = [labels[i] for i in val_idx]

    train_ds = BlockSeqDataset(train_seqs, train_labels)
    val_ds = BlockSeqDataset(val_seqs, val_labels)

    train_loader = DataLoader(
        train_ds,
        batch_size=64,
        shuffle=True,
        collate_fn=collate_fn
    )
    val_loader = DataLoader(
        val_ds,
        batch_size=64,
        shuffle=False,
        collate_fn=collate_fn
    )

    # ---- Step 4: Build and train LSTM model ----
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print("Using device:", device)

    model = LSTMPrefetcher(num_tokens=num_tokens, embed_dim=16, hidden_dim=64, num_layers=1)

    model = train_model(
        model,
        train_loader,
        val_loader,
        num_epochs=10,
        lr=1e-3,
        device=device
    )

    # ---- Step 5: Evaluate final prefetch hit-rate ----
    hit_rate = evaluate_prefetch_hit_rate(model, val_loader, device=device)
    print(f"\nFinal Prefetch Hit-Rate (val): {hit_rate:.4f}")

    # ---- Step 6: Save model ----
    model_path = "lstm_prefetch_model.pt"
    torch.save(model.state_dict(), model_path)
    print(f"Model saved → {model_path}")

    print("\n🎯 Done! LSTM model + ID mappings ready to plug into your PrefetchScheduler.")