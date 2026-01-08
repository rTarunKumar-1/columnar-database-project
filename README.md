# Adaptive Block Prefetching in Columnar Databases

This repository contains an experimental columnar storage engine that enhances query performance on Parquet files through two primary optimizations: metadata-based block pruning and intelligent, ML-driven prefetching.

The system is built on top of modern data tools:
- **PyArrow** for reading Parquet file metadata and data blocks (row groups).
- **DuckDB** for high-performance, zero-copy execution of SQL queries on in-memory Arrow data.
- **SQLGlot** for parsing and analyzing SQL queries to enable predicate pushdown.
- **PyTorch** for training and running an LSTM model that predicts future block accesses.

## Key Features

- **Micro-Block Architecture**: Leverages Parquet row groups as the fundamental unit of storage and access ("micro-blocks").
- **Metadata Indexing**: Builds an in-memory index (`MicroBlockIndex`) of all micro-blocks, caching their column-level statistics (min/max values).
- **Zone Map Pruning**: Parses `WHERE` clauses in SQL queries to compare filter conditions against the cached min/max stats, allowing the engine to skip reading blocks that cannot possibly contain relevant data.
- **ML-Based Prefetching**: An LSTM model is trained on historical query access patterns to predict which micro-blocks will be needed next.
- **Background Prefetch Service**: A background thread (`PrefetchService`) periodically runs the model on recent access history and proactively loads predicted blocks into an in-memory cache.
- **Cache-Aware Query Engine**: The main query engine (`StorageEngineV5`) is fully integrated with a cache. It serves required blocks from the cache if available and falls back to reading from disk for cache misses.

## How It Works

The system orchestrates query execution and background prefetching to minimize I/O and latency.

### Query Execution Workflow

1.  **Query Submission**: A user submits a SQL query to the `StorageEngineV5`.
2.  **Block Pruning**: The engine uses SQLGlot to parse the `WHERE` clause. It consults the `MicroBlockIndex` to identify a minimal set of candidate micro-blocks (row groups) whose min/max statistics overlap with the query's predicates.
3.  **Access Logging**: The list of candidate blocks for the query is logged to `access_log.json` and recorded in a global in-memory history. This data serves as the basis for training the prefetching model.
4.  **Cache Check**: The engine checks an in-memory LRU `BlockCache` for each required block.
5.  **Data Loading**:
    - **Cache Hit**: Blocks found in the cache are used directly (zero I/O).
    - **Cache Miss**: Blocks not in the cache are read from the Parquet file using PyArrow.
6.  **Query Processing**: All required blocks (from both cache and disk) are concatenated into a single Arrow `Table`. This table is registered with DuckDB, which executes the final query projection and aggregation with zero-copy efficiency.

### ML Prefetching Workflow

1.  **History Tracking**: The `GlobalHistory` component maintains a rolling window of the most recently accessed block IDs across all queries.
2.  **Periodic Prediction**: In the background, the `PrefetchService` wakes up at regular intervals.
3.  **Model Inference**: The service feeds the recent access sequence to the `PrefetchScheduler`, which uses the pre-trained LSTM model to predict the top-K most likely blocks to be accessed next.
4.  **Proactive Caching**: The `Prefetcher` is instructed to load these predicted blocks from the Parquet file and place them into the `BlockCache`, making them available for future queries with near-zero latency.

## Getting Started

### Prerequisites

- Python 3.9+
- The project dependencies can be installed via `pip`:
  ```bash
  pip install duckdb pandas pyarrow torch sqlglot tabulate
  ```

### Step-by-Step Usage

1.  **Prepare a Parquet File**
    Place your source data in a Parquet file. For optimal performance, convert it to a micro-block format (i.e., a Parquet file with a smaller row group size, e.g., 16,384 rows). A utility script is provided for this.
    
    *Assuming you have a large `output.parquet` file:*
    ```bash
    python parquet_to_microblocks.py
    ```
    This will create `output_microblocks.parquet`, which will be used in the next steps.

2.  **Generate Initial Access Logs**
    Before the ML model can be trained, it needs data. Run the interactive query shell and execute some queries to generate an `access_log.json` file.

    ```bash
    python run_with_prefetch_loop.py
    ```
    Inside the shell, run a few queries:
    ```sql
    SQL> select count(*) from mytable;
    SQL> select * from mytable where column1 between 10 and 20;
    SQL> select avg(column2) from mytable where column1 > 100;
    ```
    Press `CTRL+C` to exit. You should now have an `access_log.json` file.

3.  **Train the Prefetching Model**
    Now, use the generated logs to train the LSTM model.

    First, generate the training dataset:
    ```bash
    python training_set_generator.py
    ```
    This creates `training_dataset.json`.

    Next, train the model using this dataset:
    ```bash
    python retrain_model.py
    ```
    This will produce `trained_model.pt` (the model weights) and `trained_mappings.json` (the block ID to tensor index mapping).

4.  **Run Queries with Intelligent Prefetching**
    Launch the interactive shell again. This time, the `PrefetchService` will automatically load the trained model and start prefetching blocks in the background based on your query patterns.

    ```bash
    python run_with_prefetch_loop.py
    ```
    You will see logs from the engine indicating cache hits/misses and periodic logs from the `PrefetchService` as it makes predictions.

## Core Components

- `query_enginev5.py`: The primary, cache-aware storage engine that orchestrates pruning, caching, and query execution.
- `prefetch_service.py`: A background service that periodically runs the ML model to predict and prefetch blocks.
- `prefetch_scheduler.py`: Encapsulates the logic for using the trained LSTM model to suggest which blocks to prefetch based on recent history.
- `retrain_model.py`: Script to train the `LSTMPrefetcher` model using the dataset generated from access logs.
- `training_set_generator.py`: Script to process `access_log.json` and create a sliding-window dataset for model training.
- `microblock_index.py`: Defines the metadata index that holds statistics for each block, enabling query pruning.
- `block_cache.py`: A simple in-memory LRU cache for storing prefetched Arrow tables.
- `run_with_prefetch_loop.py`: An interactive shell for running SQL queries against the storage engine and observing the prefetching system in action.
- `smoke_test.py`: An end-to-end test script that verifies the entire pipeline from log generation to model training and inference.

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](LICENSE) file for details.
