import json
from typing import Dict, List, Optional, Tuple
import torch
import torch.nn.functional as F
from model import LSTMPrefetcher

class PrefetchScheduler:
    """
    Tracks recent block accesses per query and uses an LSTM model
    to predict the next likely block to prefetch.
    """
    
    def __init__(
        self,
        model: LSTMPrefetcher,
        id2idx: Dict[int, int],
        idx2id: Dict[int, int],
        vocab_size: int,  # ✅ Added explicit vocab_size
        prefetch_threshold: float = 0.6,
        max_history: int = 64,
        device: str = "cpu",
    ):
        self.model = model.to(device)
        self.model.eval()
        self.id2idx = id2idx
        self.idx2id = idx2id
        self.vocab_size = vocab_size  # ✅ Store vocab_size
        self.prefetch_threshold = prefetch_threshold
        self.max_history = max_history
        self.device = device
        self.query_history: Dict[str, List[int]] = {}
        
        #  Add UNK token handling
        self.UNK_IDX = 0  # Padding/unknown token
    
    @classmethod
    def from_files(
        cls,
        model_path: str = "trained_model.pt",
        mapping_path: str = "trained_mappings.json",
        prefetch_threshold: float = 0.6,
        max_history: int = 64,
        device: Optional[str] = None,
    ) -> "PrefetchScheduler":
        """Factory to construct scheduler from saved model and mappings."""
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        
        with open(mapping_path, "r") as f:
            mapping = json.load(f)
        
        raw_id2idx = mapping["id2idx"]
        raw_idx2id = mapping["idx2id"]
        id2idx = {int(k): int(v) for k, v in raw_id2idx.items()}
        idx2id = {int(k): int(v) for k, v in raw_idx2id.items()}
        
        # FIX: Load vocab_size from mapping instead of deriving
        vocab_size = int(mapping.get("vocab_size", max(idx2id.keys()) + 1))
        
        print(f"[Scheduler] Loaded vocabulary with {len(id2idx)} block IDs")
        print(f"[Scheduler] Vocab size: {vocab_size}")
        print(f"[Scheduler] Block ID range: {min(id2idx.keys())} to {max(id2idx.keys())}")
        
        #  Use vocab_size directly
        model = LSTMPrefetcher(
            num_tokens=vocab_size,
            embed_dim=16,
            hidden_dim=64,
            num_layers=1,
        )
        
        state_dict = torch.load(model_path, map_location=device)
        model.load_state_dict(state_dict)
        
        return cls(
            model=model,
            id2idx=id2idx,
            idx2id=idx2id,
            vocab_size=vocab_size,  # ✅ Pass vocab_size
            prefetch_threshold=prefetch_threshold,
            max_history=max_history,
            device=device,
        )
    
    def register_access(self, query_id: str, block_id: int) -> None:
        """Register block access for query history."""
        history = self.query_history.setdefault(query_id, [])
        history.append(int(block_id))
        if len(history) > self.max_history:
            self.query_history[query_id] = history[-self.max_history:]
    
    def suggest_topk_prefetch(
        self,
        query_id: str,
        sequence: Optional[List[int]] = None,
        k: int = 10,
        exclude_blocks: Optional[set] = None
    ) -> Optional[List[Tuple[int, float]]]:
        """
        Returns top-K blocks using SIGMOID (multi-label).
        """
        if sequence is not None:
            history = sequence
        else:
            history = self.query_history.get(query_id, [])
        
        print(f"[Scheduler DEBUG] query_id={query_id}, history length={len(history)}")
        
        if len(history) < 3:
            print(f"[Scheduler DEBUG] History too short: {len(history)} < 3")
            return None
        
        seq_idx = self._encode_sequence(history)
        if seq_idx is None or len(seq_idx) == 0:
            print(f"[Scheduler DEBUG] Failed to encode sequence or empty after UNK filtering")
            return None
        
        seq_tensor = torch.tensor([seq_idx], dtype=torch.long, device=self.device)
        lengths = torch.tensor([len(seq_idx)], dtype=torch.long, device=self.device)
        
        with torch.no_grad():
            logits = self.model(seq_tensor, lengths)  # [1, vocab_size]
            
            #  FIX: Mask padding token before sigmoid
            logits[:, 0] = -1e9  # Force pad to never be chosen
            
            # Use sigmoid for multi-label
            probs = torch.sigmoid(logits)  # [1, vocab_size]
        
        # Get top-K predictions
        topk_probs, topk_indices = torch.topk(probs[0], k=min(k, probs.size(1)))
        
        exclude_set = exclude_blocks or set()
        results = []
        
        print(f"[Scheduler DEBUG] Raw top-{k} probabilities (sigmoid):")
        for i, (prob, idx) in enumerate(zip(topk_probs, topk_indices)):
            confidence = float(prob.item())
            pred_idx = int(idx.item())
            
            # Skip padding token (already masked but double-check)
            if pred_idx == 0:
                continue
            
            # Convert index to block ID
            block_id = self.idx2id.get(pred_idx)
            if block_id is None:
                print(f"  Rank {i+1}: idx={pred_idx} NOT IN VOCAB")
                continue
            
            # Skip cached blocks
            if block_id in exclude_set:
                print(f"  Rank {i+1}: block={block_id} CACHED, skipping")
                continue
            
            results.append((block_id, confidence))
            print(f"  Rank {i+1}: block={block_id}, prob={confidence:.6f}")
            
            if len(results) >= k:
                break
        
        print(f"[Scheduler DEBUG] Returning {len(results)} blocks")
        return results if results else None
    
    def _encode_sequence(self, history: List[int]) -> Optional[List[int]]:
        """
        Maps raw block ids to token indices.
        FIX: Handle unseen blocks gracefully instead of failing.
        """
        seq_idx = []
        for bid in history[-64:]:  # Take last 64 blocks
            idx = self.id2idx.get(bid, self.UNK_IDX)  #  Map unseen to UNK
            seq_idx.append(idx)
        
        # Filter out UNK tokens to avoid noise
        seq_idx = [x for x in seq_idx if x != self.UNK_IDX]
        
        # Return None if sequence is too short after filtering
        if len(seq_idx) < 3:
            return None
        
        return seq_idx
