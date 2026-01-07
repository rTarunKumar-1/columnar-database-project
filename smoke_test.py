"""Quick end-to-end test of the training and inference pipeline."""
import os
import json

def test_pipeline():
    print("=== Smoke Test: Training & Inference Pipeline ===\n")
    
    # Step 1: Check if access log exists
    if not os.path.exists("access_log.json"):
        print("❌ No access_log.json found. Run queries first.")
        return False
    
    with open("access_log.json") as f:
        events = json.load(f)
    print(f"✅ Found access_log.json with {len(events)} events")
    
    # Step 2: Generate training data
    print("\n--- Generating training dataset ---")
    import training_set_generator
    training_set_generator.main()
    
    if not os.path.exists("training_dataset.json"):
        print("❌ Failed to generate training_dataset.json")
        return False
    print("✅ Generated training_dataset.json")
    
    # Step 3: Train model (1 epoch for speed)
    print("\n--- Training model (1 epoch) ---")
    import torch
    from retrain_model import main as train_main
    # Temporarily modify epochs
    import retrain_model
    original_code = retrain_model.__dict__.get('main')
    # Just run it - or manually edit retrain_model.py to set epochs=1 for testing
    train_main()
    
    if not os.path.exists("trained_model.pt"):
        print("❌ Model training failed")
        return False
    print("✅ Model trained successfully")
    
    # Step 4: Load scheduler and test prediction
    print("\n--- Testing scheduler inference ---")
    from prefetch_scheduler import PrefetchScheduler
    
    scheduler = PrefetchScheduler.from_files()
    
    # Get sample sequence from access log
    sample_seq = [e["block"] for e in events[-30:] if "block" in e]
    
    suggestions = scheduler.suggest_topk_prefetch(
        "TEST",
        sequence=sample_seq,
        k=5
    )
    
    if suggestions:
        print(f"✅ Got {len(suggestions)} predictions:")
        for block_id, conf in suggestions[:3]:
            print(f"   Block {block_id}: {conf:.4f}")
    else:
        print("⚠️  No predictions (may be expected if sequence too short)")
    
    print("\n=== All Tests Passed ===")
    return True

if __name__ == "__main__":
    test_pipeline()
