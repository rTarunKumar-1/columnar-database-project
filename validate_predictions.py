# validate_predictions.py

import json
import torch
from model import LSTMPrefetcher
from access_logger import AccessLogger


def main():
    logger = AccessLogger()
    events = logger.get_all_events()

    if not events:
        print("no history found")
        return

    with open("trained_mappings.json", "r") as f:
        mapping = json.load(f)

    id2idx = mapping["id2idx"]
    idx2id = {int(k): int(v) for k,v in mapping["idx2id"].items()}

    model = LSTMPrefetcher(
        num_blocks=len(id2idx),
        embed_dim=64,
        hidden_dim=128
    )
    model.load_state_dict(torch.load("trained_model.pt"))
    model.eval()

    seq = []
    for e in events[-1]["blocks"]:
        seq.append(e)

    enc = [id2idx[b] for b in seq if b in id2idx]

    if len(enc) < 5:
        print("not enough real data for prediction test")
        return

    x = torch.tensor(enc).unsqueeze(0)

    with torch.no_grad():
        logits = model(x)
        prob = torch.softmax(logits, dim=-1)
        idx = torch.argmax(prob).item()

    predicted_block = idx2id[idx]
    print(f"model predicted next block: {predicted_block}")


if __name__ == "__main__":
    main()
