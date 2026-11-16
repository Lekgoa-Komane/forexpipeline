# ForexPipeline: Multi-Timeframe Quantitative Trading & Targets Orchestration Engine

`forexpipeline` is the master orchestration repository for an institutional-grade algorithmic Foreign Exchange, Commodity, and Metal trading system. Powered by the R `targets` package, it coordinates data extraction, time-series aggregation, Smart Money Concepts (SMC) structural analysis, signal generation, historical backtesting, Parquet pantry storage, and SQLite trade journaling across two core companion packages: `forexdatasets` and `forexengine`.

---

## Pipeline Architecture Overview

The `targets` workflow manages a complex, directed acyclic graph (DAG) that ensures efficient, reproducible, and cached data processing across multiple frequencies (Hourly, 4-Hour, Daily, Weekly):

```
[Config & COT Reports] ──> [Extraction (Local/Stooq Web)] ──> [Updates & TF Aggregation] ──> 
[Institutional Mapping (W/D/H4/H1)] ──> [Reversal Watch & Context] ──> [Signals & Backtest] ──> 
[Parquet Storage & SQLite Journal Sync]
```

1. **Configuration & Metadata Initialization**: Loads and validates pipeline parameters from `config.yml`[cite: 8] and builds an exhaustive ticker-level configuration matrix (`ticker_meta`)[cite: 8].
2. **Extraction Stage**: Acquires Commodity Futures Trading Commission (CFTC) Commitments of Traders (COT) reports[cite: 8], parses historical hourly records from local databases[cite: 8], and dynamically routes daily asset feeds between local Stooq snapshots and automated web scrapers[cite: 8].
3. **Update & Timeframe Aggregation**: Processes raw data tables against existing Parquet archives (`update_h1_data`, `update_daily_data`)[cite: 8] and builds higher-timeframe candles (4-Hour and Weekly)[cite: 8].
4. **Institutional Mapping & Context Engine (`forexengine`)**: Executes multi-timeframe SMC mapping across weekly, daily, H4, and H1 frequencies, runs Stage 1 reversal watches, and aligns market context to produce high-confluence execution tables[cite: 8].
5. **Signal Generation & Backtesting**: Computes entry, stop-loss, take-profit, and risk-reward metrics to generate trade signals (`signals_generator`), subsequently evaluating performance through swing-based trailing stops and structural exits (`backtest_analysis`)[cite: 8].
6. **Persistence & Journaling**: Persists all mutated datasets, registries, BOS/CHoCH state-machine logs, and reversal watch tables to Zstandard-compressed Apache Parquet partitions, while automatically synchronizing backtest outcomes and paper-trading logs into local SQLite databases[cite: 8].

---

## Pipeline Target Breakdown (`_targets.R`)

* **Configuration & Metadata**:
  * `config_path`[cite: 8]: Tracks `config.yml` as a file target to automatically invalidate downstream caches when parameters change.
  * `config`[cite: 8] / `ticker_meta`[cite: 8]: Validates and expands configuration rules into runtime metadata.
* **Extraction Targets**:
  * `cot_report`[cite: 8]: Fetches and caches CFTC COT reports (refreshed every 6 days via `tar_cue_age`).
  * `h1_table` / `daily_table`[cite: 8]: Dynamic branch-and-bound loops extracting hourly and daily records from local storage and Stooq web sources.
* **Update & Aggregation Targets**:
  * `h1_data`, `h4_data`, `daily_data`, `weekly_data`[cite: 8]: Cleans, deduplicates, and aggregates multi-frequency time-series data while preserving time-series continuity.
* **Engine & Analysis Targets**:
  * `weekly_mutate`, `daily_mutate`, `h4_mutate`, `h1_mutate`[cite: 8]: Executes single-timeframe institutional mapping (swings, FVGs, order blocks, BOS/CHoCH state machines).
  * `reversal_watch`[cite: 8]: Early-warning detector spotting higher-timeframe zone rejections and H1 CHoCH confirmations.
  * `market_context_result`[cite: 8]: Combines multi-timeframe bias, deal ranges, DOL targets, and DOT triggers onto H1 bars.
  * `signals_result` & `backtest_results`[cite: 8]: Generates trade signal logs and simulates historical trade outcomes.
* **Storage & Journaling Targets**:
  * `save_*_data` / `save_*_registry` / `save_*_rev_watch` / `save_*_boschoch`[cite: 8]: Atomic Parquet writes using `.tmp` file swaps for fast analytical serving.
  * `journal_sync`[cite: 8]: Opens a connection to the SQLite trading journal (`connect_journal_db`) and atomically synchronizes backtest results via `sync_journal_from_bt()` while preserving manual annotations (`emotion_rating`, `notes`, `tags`).
  * `paper_trade_sync`[cite: 8]: Initializes and retrieves live paper-trading states from the paper-trading SQLite database.

---

## Installation & Requirements

### Prerequisites
* R (version 4.0 or higher recommended)
* Required Core Packages:
  * `targets`[cite: 8] (Pipeline orchestration)
  * `tarchetypes`[cite: 8] (Advanced target definitions like age-based caching)
  * `data.table`[cite: 8] (High-performance data manipulation)
  * `DBI` & `RSQLite`[cite: 8] (Database connectivity for journals)
  * `forexdatasets`[cite: 8] (Data ingestion, transformation, and Parquet storage)
  * `forexengine`[cite: 8] (Institutional mapping, context, and signal evaluation)

### Running the Pipeline

1. Ensure your local Stooq directories and configuration files (`config.yml`) are properly positioned as expected by `forexdatasets`.
2. Open your R session inside the `forexpipeline` project root.
3. Inspect the pipeline DAG to ensure all targets are valid:
   ```r
   targets::tar_manifest()
   targets::tar_network()
   ```
4. Execute the pipeline:
   ```r
   targets::tar_make()
   ```
5. Review results or query the SQLite trade journal:
   ```r
   library(DBI)
   # Load config and inspect journal trades
   config <- forexdatasets::load_config()
   con <- forexengine::connect_journal_db(config)
   trades <- forexengine::get_journal_trades(con, status = "closed")
   dbDisconnect(con)
   head(trades)
   ```

---

## Error Handling & Best Practices

* **Incremental Caching**: Because data extraction and institutional mapping are computationally expensive, `targets` automatically tracks file hashes and modification timestamps. Unchanged targets are instantly skipped on subsequent `tar_make()` runs.
* **Age-Based Caching**: Volatile external feeds (such as Stooq daily web scrapes, COT reports, and raw data saves) incorporate 6-day age-caching rules (`tar_cue_age`) to balance freshness with unnecessary network/disk overhead[cite: 8].
* **Transactional Safety**: Database operations (such as syncing backtest logs into the SQLite journal) are wrapped in atomic transactions (`dbBegin` / `dbCommit` / `dbRollback`), ensuring that pipeline interruptions never corrupt historical records or wipe out manual user notes.