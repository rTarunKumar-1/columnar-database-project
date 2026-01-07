import json
import torch
from torch.utils.data import TensorDataset, DataLoader
from model import LSTMPrefetcher

def topk_hit_rate(model, loader, k=10, device="cpu"):
    """
    Compute top-k hit rate: fraction of times the true label
    appears in the top-k predictions.
    """
    model.eval()
    hits = 0
    total = 0
    
    with torch.no_grad():
        for batch_x, batch_y_multihot in loader:
            batch_x = batch_x.to(device)
            # Get original single labels from multihot
            labels = batch_y_multihot.argmax(dim=1)  # [B]
            
            lengths = torch.full((batch_x.size(0),), batch_x.size(1), dtype=torch.long).to(device)
            logits = model(batch_x, lengths)  # [B, V]
            
            # Mask padding
            logits[:, 0] = -1e9
            probs = torch.sigmoid(logits)  # [B, V]
            
            topk_indices = torch.topk(probs, k, dim=1).indices  # [B, k]
            
            for i in range(labels.size(0)):
                if labels[i].item() in topk_indices[i]:
                    hits += 1
                total += 1
    
    return hits / total if total > 0 else 0.0

def mean_reciprocal_rank(model, loader, device="cpu"):
    """Compute MRR: average of 1/rank where rank is position of true label."""
    model.eval()
    reciprocal_ranks = []
    
    with torch.no_grad():
        for batch_x, batch_y_multihot in loader:
            batch_x = batch_x.to(device)
            labels = batch_y_multihot.argmax(dim=1)
            
            lengths = torch.full((batch_x.size(0),), batch_x.size(1), dtype=torch.long).to(device)
            logits = model(batch_x, lengths)
            
            logits[:, 0] = -1e9
            probs = torch.sigmoid(logits)
            
            # Get full ranking
            sorted_indices = torch.argsort(probs, dim=1, descending=True)  # [B, V]
            
            for i in range(labels.size(0)):
                true_label = labels[i].item()
                # Find rank of true label (1-indexed)
                rank = (sorted_indices[i] == true_label).nonzero(as_tuple=True)[0].item() + 1
                reciprocal_ranks.append(1.0 / rank)
    
    return sum(reciprocal_ranks) / len(reciprocal_ranks) if reciprocal_ranks else 0.0

def main():
    # Load dataset
    with open("training_dataset.json", "r") as f:
        data = json.load(f)
    
    X = torch.tensor(data["inputs"], dtype=torch.long)
    Y = torch.tensor(data["labels"], dtype=torch.long)
    vocab_size = int(data["vocab_size"])
    
    # Convert to multihot
    Y_multihot = torch.zeros(Y.size(0), vocab_size, dtype=torch.float)
    Y_multihot[torch.arange(Y.size(0)), Y] = 1.0
    
    dataset = TensorDataset(X, Y_multihot)
    loader = DataLoader(dataset, batch_size=32, shuffle=False)
    
    # Load model
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = LSTMPrefetcher(num_tokens=vocab_size, embed_dim=16, hidden_dim=64, num_layers=1)
    model.load_state_dict(torch.load("trained_model.pt", map_location=device))
    model.to(device)
    
    # Compute metrics
    print("Computing evaluation metrics...")
    top1 = topk_hit_rate(model, loader, k=1, device=device)
    top3 = topk_hit_rate(model, loader, k=3, device=device)
    top5 = topk_hit_rate(model, loader, k=5, device=device)
    top10 = topk_hit_rate(model, loader, k=10, device=device)
    mrr = mean_reciprocal_rank(model, loader, device=device)
    
    print(f"\nTop-1 Hit Rate: {top1:.4f}")
    print(f"Top-3 Hit Rate: {top3:.4f}")
    print(f"Top-5 Hit Rate: {top5:.4f}")
    print(f"Top-10 Hit Rate: {top10:.4f}")
    # print(f"Mean Reciprocal Rank: {mrr:.4f}")

if __name__ == "__main__":
    main()
