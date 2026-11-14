# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
targets::tar_option_set(
  packages = c("DBI", "forexdatasets", "forexengine") # Packages that your targets need for their tasks.
  # format = "qs", # Optionally set the default storage format. qs is fast.
  #
  # Pipelines that take a long time to run may benefit from
  # optional distributed computing. To use this capability
  # in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller that scales up to a maximum of two workers
  # which run as local R processes. Each worker launches when there is work
  # to do and exits if 60 seconds pass with no tasks to run.
  #
  #   controller = crew::crew_controller_local(workers = 2, seconds_idle = 60)
  #
  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package.
  # For the cloud, see plugin packages like {crew.aws.batch}.
  # The following example is a controller for Sun Grid Engine (SGE).
  #
  #   controller = crew.cluster::crew_controller_sge(
  #     # Number of workers that the pipeline can scale up to:
  #     workers = 10,
  #     # It is recommended to set an idle time so workers can shut themselves
  #     # down if they are not running tasks.
  #     seconds_idle = 120,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific verison of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.2".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )
  #
  # Set other options as needed.
)

# Run the R scripts in the R/ folder with your custom functions:
targets::tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.

# Replace the target list below with your own:
list(
  targets::tar_target(
    # Track the configuration file as a file target
    # so the pipeline reruns when the YAML changes.
    name = config_path,
    command = system.file("config", "config.yml", package = "forexdatasets"),
    format = "file"
  ),
  # Load and validate the configuration
  targets::tar_target(
    name = config,
    command = load_config(config_path)
  ),
  # Build ticker metadata from the config
  targets::tar_target(
    name = ticker_meta,
    command = ticker_metadata(config)
  ),
  # ==============================
  # C.O.T REPORT TARGET
  # ==============================
  targets::tar_target(
    name = cot_report,
    command = cot_pipeline(config),
    cue = tarchetypes::tar_cue_age(
      name = cot_report,
      age = as.difftime(6, units = "days")
    )
  ),
  # ==============================
  # EXTRACTION STAGE TARGETS
  # ==============================
  # Filtered h1 metadata as its own target (static, no pattern)
  tar_target(
    name = h1_meta,
    command = ticker_meta[frequency == "hourly"]
  ),
  # Tracking files from the manually downloaded stooq database by
  # Branching over h1_meta to get paths, one branch per ticker
  targets::tar_target(
    name = h1_paths,
    command = h1_meta$source_path,
    pattern = map(h1_meta),
    format = "file"
  ),
  # extract required data from the database
  targets::tar_target(
    name = h1_list,
    command = extract_hourly_data(h1_meta, h1_paths, config),
    pattern = map(h1_meta, h1_paths),
    iteration = "list"
  ),
  # Bind the h1_list into one data.table
  targets::tar_target(
    name = h1_table,
    command = data.table::rbindlist(h1_list, use.names = TRUE, fill = TRUE)
  ),
  # Filtered daily metadata as its own target (static, no pattern)
  tar_target(
    name = daily_meta,
    command = ticker_meta[
      frequency == "daily" & data_source %in% c("local", "local_stooq")
    ]
  ),
  # Filtered stooq metadata as its own target (static, no pattern)
  targets::tar_target(
    name = stooq_meta,
    command = ticker_meta[
      asset_class %in%
        c("Commodity_Futures", "Currency_Futures") &
        data_source == "stooq"
    ]
  ),
  # Tracking files from the manually downloaded stooq database by
  # Branching over daily_meta to get paths, one branch per ticker
  targets::tar_target(
    name = daily_paths,
    command = daily_meta$source_path,
    pattern = map(daily_meta),
    format = "file"
  ),
  # Branching over stooq_meta to get paths, one branch per ticker
  targets::tar_target(
    name = stooq_paths,
    command = stooq_meta$source_path,
    pattern = map(stooq_meta)
  ),
  # Web scrape futures data from stooq website
  targets::tar_target(
    name = stooq_data,
    command = extract_daily_data(stooq_meta, stooq_paths, config),
    pattern = map(stooq_meta, stooq_paths),
    iteration = "list",
    cue = tarchetypes::tar_cue_age(
      name = stooq_data,
      age = as.difftime(6, units = "days")
    )
  ),
  # extract required data from the database
  targets::tar_target(
    name = daily_list,
    command = {
      stooq_data # just naming it makes daily_list wait for it
      extract_daily_data(daily_meta, daily_paths, config)
    },
    pattern = map(daily_meta, daily_paths),
    iteration = "list"
  ),

  # Bind the daily_list into one data.table
  targets::tar_target(
    name = daily_table,
    command = data.table::rbindlist(daily_list, use.names = TRUE, fill = TRUE)
  ),
  # ==============================
  # UPDATE STAGE TARGETS
  # ==============================
  targets::tar_target(
    name = h1_data,
    command = update_h1_data(h1_table, config)
  ),

  targets::tar_target(
    name = h4_data,
    command = aggregate_hourly_to_4hour(h1_data, config)
  ),

  targets::tar_target(
    name = daily_data,
    command = update_daily_data(daily_table, config)
  ),

  targets::tar_target(
    name = weekly_data,
    command = aggregate_daily_to_weekly(daily_data, config)
  ),
  # ==============================
  # FOREXENGINE TARGETS
  # ==============================
  # 1. Institutional Mapping per timeframe (consuming saved/updated datasets)
  targets::tar_target(
    name = weekly_mutate,
    command = institutional_mapping(
      weekly_data[asset_class %in% c("Currency_Pairs", "Metals")],
      config
    )
  ),
  targets::tar_target(
    name = daily_mutate,
    command = institutional_mapping(
      daily_data[asset_class %in% c("Currency_Pairs", "Metals")],
      config
    )
  ),
  targets::tar_target(
    name = h4_mutate,
    command = institutional_mapping(
      h4_data[asset_class %in% c("Currency_Pairs", "Metals")],
      config
    )
  ),
  targets::tar_target(
    name = h1_mutate,
    command = institutional_mapping(
      h1_data[asset_class %in% c("Currency_Pairs", "Metals")],
      config
    )
  ),
  # 2. Reversal Watch Computation
  targets::tar_target(
    name = reversal_watch,
    command = compute_reversal_watch(
      weekly_mutated = weekly_mutate,
      daily_mutated = daily_mutate,
      h4_mutated = h4_mutate,
      h1_mutated = h1_mutate
    )
  ),
  # 3. Market Context & Execution Table Generation
  targets::tar_target(
    name = market_context_result,
    command = market_context(
      weekly_mutate = weekly_mutate,
      daily_mutate = daily_mutate,
      h4_mutate = h4_mutate,
      h1_mutate = h1_mutate,
      reversal_watch = reversal_watch,
      config = config
    )
  ),
  # 4. Signals Generation & Backtest Analysis
  targets::tar_target(
    name = signals_result,
    command = signals_generator(market_context_result, config)
  ),
  targets::tar_target(
    name = backtest_results,
    command = backtest_analysis(signals_result, config)
  ),
  # ==============================
  # FOREXDATASETS PARQUET STORAGE TARGETS
  # ==============================
  targets::tar_target(
    name = save_h1_data,
    command = save_dataset(h1_data, config),
    cue = tarchetypes::tar_cue_age(
      name = save_h1_data,
      age = as.difftime(6, units = "days")
    )
  ),
  targets::tar_target(
    name = save_h4_data,
    command = save_dataset(h4_data, config),
    cue = tarchetypes::tar_cue_age(
      name = save_h4_data,
      age = as.difftime(6, units = "days")
    )
  ),
  targets::tar_target(
    name = save_daily_data,
    command = save_dataset(daily_data, config),
    cue = tarchetypes::tar_cue_age(
      name = save_daily_data,
      age = as.difftime(6, units = "days")
    )
  ),
  targets::tar_target(
    name = save_weekly_data,
    command = save_dataset(weekly_data, config),
    cue = tarchetypes::tar_cue_age(
      name = save_weekly_data,
      age = as.difftime(6, units = "days")
    )
  ),
  targets::tar_target(
    name = save_cot_data,
    command = save_cot_dataset(cot_report, config),
    cue = tarchetypes::tar_cue_age(
      name = save_cot_data,
      age = as.difftime(6, units = "days")
    )
  ),
  # ==============================
  # FOREXENGINE PARQUET STORAGE TARGETS
  # ==============================
  # Save registry tables or engine results using forexengine's `save_result()`
  targets::tar_target(
    name = save_weekly_registry,
    command = save_result(
      weekly_mutate$weekly_registry,
      table = "registry",
      config = config
    )
  ),
  targets::tar_target(
    name = save_daily_registry,
    command = save_result(
      daily_mutate$daily_registry,
      table = "registry",
      config = config
    )
  ),
  targets::tar_target(
    name = save_h4_registry,
    command = save_result(
      h4_mutate$h4_registry,
      table = "registry",
      config = config
    )
  ),
  targets::tar_target(
    name = save_h1_registry,
    command = save_result(
      h1_mutate$h1_registry,
      table = "registry",
      config = config
    )
  ),
  # save reversal watch tables using forexengine's `save_result()`
  targets::tar_target(
    name = save_weekly_rev_watch,
    command = save_result(
      reversal_watch$weekly_final_dt,
      table = "reversal_watch",
      config = config
    )
  ),
  targets::tar_target(
    name = save_daily_rev_watch,
    command = save_result(
      reversal_watch$daily_final_dt,
      table = "reversal_watch",
      config = config
    )
  ),
  # save bos_choch tables using forexengine's `save_result()`
  targets::tar_target(
    name = save_weekly_boschoch,
    command = save_result(
      weekly_mutate$weekly_data,
      table = "bos_choch",
      config = config
    )
  ),
  targets::tar_target(
    name = save_daily_boschoch,
    command = save_result(
      daily_mutate$daily_data,
      table = "bos_choch",
      config = config
    )
  ),
  targets::tar_target(
    name = save_h4_boschoch,
    command = save_result(
      h4_mutate$h4_data,
      table = "bos_choch",
      config = config
    )
  ),
  targets::tar_target(
    name = save_h1_boschoch,
    command = save_result(
      h1_mutate$h1_data,
      table = "bos_choch",
      config = config
    )
  ),
  # Sync the backtest results to your SQLite trading journal database
  targets::tar_target(
    name = journal_sync,
    command = {
      con <- connect_journal_db(config)
      on.exit(dbDisconnect(con), add = TRUE)
      sync_journal_from_bt(con, backtest_results)
    }
  ),
  targets::tar_target(
    name = paper_trade_sync,
    command = {
      con <- init_paper_trading_db(config)
      on.exit(dbDisconnect(con), add = TRUE)
      get_paper_trades(con)
    }
  )
)
