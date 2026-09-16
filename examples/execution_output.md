# Biflux Terminal Execution Screenshot: Train-Serve Skew Elimination

This terminal transcript serves as the visual verification of **Project Biflux** in action. It executes `examples/bond_pricing_pipeline.py`, which runs the identical Python Polars transformation graph against both historical lakehouse batch storage and live streaming micro-batches, proving zero Train-Serve Skew down to the decimal point.

```terminal
$ python examples/bond_pricing_pipeline.py
================================================================================
  PROJECT BIFLUX: TRAIN-SERVE SKEW ELIMINATION VERIFICATION
================================================================================
[*] Generating 20,000 synthetic bond market quotes across 5 assets...
[*] Dataset schema: {'symbol': 'String', 'bid': 'Float64', 'ask': 'Float64', 'size': 'Int64', 'timestamp': 'Int64'}
[*] Sample head:
shape: (3, 5)
┌────────────────┬──────────┬──────────┬──────┬───────────────┐
│ symbol         ┆ bid      ┆ ask      ┆ size ┆ timestamp     │
│ ---            ┆ ---      ┆ ---      ┆ ---  ┆ ---           │
│ str            ┆ f64      ┆ f64      ┆ i64  ┆ i64           │
╞════════════════╪══════════╪══════════╪══════╪═══════════════╡
│ AAPL-CORP-2030 ┆ 98.5289  ┆ 98.5827  ┆ 500  ┆ 1700000000000 │
│ AMZN-CORP-2029 ┆ 100.5898 ┆ 100.6607 ┆ 100  ┆ 1700000000050 │
│ GOOG-CORP-2032 ┆ 95.7534  ┆ 95.9372  ┆ 100  ┆ 1700000000100 │
└────────────────┴──────────┴──────────┴──────┴───────────────┘

--------------------------------------------------------------------------------
>>> [STEP A] RUNNING HISTORICAL BATCH BACKTEST (S3 / ICEBERG / PARQUET)
--------------------------------------------------------------------------------
[✓] Batch Backtest Completed in 10 ms
[✓] Output Rows Written: 5
[✓] Batch VWAP Feature Table:
shape: (5, 6)
┌────────────────┬──────────────┬─────────────────────┬────────────┬──────────┬──────────┐
│ symbol         ┆ total_volume ┆ total_dollar_volume ┆ vwap       ┆ min_bid  ┆ max_ask  │
│ ---            ┆ ---          ┆ ---                 ┆ ---        ┆ ---      ┆ ---      │
│ str            ┆ i64          ┆ f64                 ┆ f64        ┆ f64      ┆ f64      │
╞════════════════╪══════════════╪═════════════════════╪════════════╪══════════╪══════════╡
│ AAPL-CORP-2030 ┆ 6217450      ┆ 6.1123e8            ┆ 98.308978  ┆ 97.2502  ┆ 99.4306  │
│ AMZN-CORP-2029 ┆ 6207250      ┆ 6.2781e8            ┆ 101.142059 ┆ 100.1009 ┆ 102.2982 │
│ GOOG-CORP-2032 ┆ 6393450      ┆ 6.1022e8            ┆ 95.443961  ┆ 94.4009  ┆ 96.5958  │
│ MSFT-CORP-2028 ┆ 6466000      ┆ 6.7769e8            ┆ 104.808747 ┆ 103.7516 ┆ 105.9398 │
│ US-TREAS-10Y   ┆ 6352000      ┆ 6.3422e8            ┆ 99.846180  ┆ 98.8001  ┆ 100.9859 │
└────────────────┴──────────────┴─────────────────────┴────────────┴──────────┴──────────┘

--------------------------------------------------------------------------------
>>> [STEP B] RUNNING LIVE STREAM PROCESSING (KAFKA MICRO-BATCHES)
--------------------------------------------------------------------------------
[*] Processing 10 live micro-batches via Arrow memory buffers...
[✓] Live Stream Execution Completed in 10 ms
[✓] Live Streaming VWAP Feature Table:
shape: (5, 6)
┌────────────────┬──────────────┬─────────────────────┬────────────┬──────────┬──────────┐
│ symbol         ┆ total_volume ┆ total_dollar_volume ┆ vwap       ┆ min_bid  ┆ max_ask  │
│ ---            ┆ ---          ┆ ---                 ┆ ---        ┆ ---      ┆ ---      │
│ str            ┆ i64          ┆ f64                 ┆ f64        ┆ f64      ┆ f64      │
╞════════════════╪══════════════╪═════════════════════╪════════════╪══════════╪══════════╡
│ AAPL-CORP-2030 ┆ 6217450      ┆ 6.1123e8            ┆ 98.308978  ┆ 97.2502  ┆ 99.4306  │
│ AMZN-CORP-2029 ┆ 6207250      ┆ 6.2781e8            ┆ 101.142059 ┆ 100.1009 ┆ 102.2982 │
│ GOOG-CORP-2032 ┆ 6393450      ┆ 6.1022e8            ┆ 95.443961  ┆ 94.4009  ┆ 96.5958  │
│ MSFT-CORP-2028 ┆ 6466000      ┆ 6.7769e8            ┆ 104.808747 ┆ 103.7516 ┆ 105.9398 │
│ US-TREAS-10Y   ┆ 6352000      ┆ 6.3422e8            ┆ 99.846180  ┆ 98.8001  ┆ 100.9859 │
└────────────────┴──────────────┴─────────────────────┴────────────┴──────────┴──────────┘

================================================================================
>>> [THE PROOF] COMPARING BATCH VS STREAM FEATURE VECTORS
================================================================================
SYMBOL             | BATCH VWAP     | LIVE VWAP      | DIFF         | STATUS
--------------------------------------------------------------------------------
AAPL-CORP-2030     | 98.308978      | 98.308978      | 0.000000     | MATCH (0.000000 Skew)
AMZN-CORP-2029     | 101.142059     | 101.142059     | 0.000000     | MATCH (0.000000 Skew)
GOOG-CORP-2032     | 95.443961      | 95.443961      | 0.000000     | MATCH (0.000000 Skew)
MSFT-CORP-2028     | 104.808747     | 104.808747     | 0.000000     | MATCH (0.000000 Skew)
US-TREAS-10Y       | 99.846180      | 99.846180      | 0.000000     | MATCH (0.000000 Skew)
--------------------------------------------------------------------------------
>>> VERIFICATION SUCCESS: Zero Train-Serve Skew Detected.
>>> Single Python Polars transformation produced identical results in both modes.
```
