"""
Daily BigQuery export of poly-trader analytics tables.

Runs once per day at 00:30 UTC (just after UTC midnight). Dumps three
tables to BigQuery:
  • poly_trader_analytics.decisions          — every copy + resolution
  • poly_trader_analytics.polygon_stream_hits — every on-chain whale hit
  • poly_trader_analytics.smart_wallet_rankings — daily snapshot of roster

Why daily not streaming: bot-side cost is minimal (1 export/day), no
per-row streaming insert charges, BigQuery free tier (10 GB storage +
1 TB query/mo) handles our volume easily. For real-time querying later
we can add streaming inserts via the BigQuery Storage Write API.

User can query via:
  • BigQuery console: https://console.cloud.google.com/bigquery
  • bq command line
  • Looker Studio (free) for charts on top of the dataset

Sample analytics queries are in INFRASTRUCTURE.md.
"""
import logging
import os
import tempfile
from datetime import datetime

import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

PROJECT = "crypto-agent-494710"
DATASET = "poly_trader_analytics"
TABLES = ["decisions", "polygon_stream_hits", "smart_wallet_rankings",
          "rejected_decisions"]


def _ensure_dataset(client):
    """Create the BigQuery dataset if it doesn't exist."""
    from google.cloud import bigquery
    ds_ref = f"{PROJECT}.{DATASET}"
    try:
        client.get_dataset(ds_ref)
    except Exception:
        dataset = bigquery.Dataset(ds_ref)
        dataset.location = "US"
        dataset.description = "poly-trader smart-money copy strategy analytics"
        client.create_dataset(dataset)
        logger.info(f"bq_export: created dataset {ds_ref}")


def _table_to_dataframe(engine, table: str) -> pd.DataFrame:
    """Pull entire table to a pandas DataFrame. Tables are small (decisions
    is a few hundred rows, stream_hits last 500, rankings ~20)."""
    with Session(engine) as session:
        return pd.read_sql(f"SELECT * FROM {table}", session.bind)


def _df_to_bigquery(client, df: pd.DataFrame, table: str) -> int:
    """WRITE_TRUNCATE the BigQuery table with the SQLite contents.
    Returns row count loaded."""
    from google.cloud import bigquery
    if df.empty:
        logger.info(f"bq_export: {table} is empty, skipping")
        return 0
    table_ref = f"{PROJECT}.{DATASET}.{table}"
    # Sanitize: convert datetime columns to ISO strings so BigQuery can
    # auto-detect them. pandas datetime64 to_gbq has known issues.
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce").astype(str)
    # Force-coerce known-string columns to str so autodetect doesn't infer
    # them as FLOAT64. condition_id and 0xWALLET addresses look numeric in
    # rare cases (all-hex-digit substrings) and the autodetector picks the
    # narrowest type. condition_id storing wallets via FLOAT64 loses
    # precision past 2^53 — symptoms observed in production exports
    # (2026-05-19).
    _STRING_COLS = {
        "condition_id", "source_wallet", "wallet", "tx_hash", "pseudonym",
        "strategy", "decision_outcome", "edge_definition", "side", "mode",
        "notes", "reject_reason", "reject_detail", "asset_id",
        "yes_token_id", "no_token_id", "category", "question",
    }
    for col in df.columns:
        if col in _STRING_COLS:
            # Convert to nullable string: NaN → empty string for now, then
            # the _clean() helper below maps empty-or-NaN back to None
            # before serialization.
            df[col] = df[col].where(pd.notna(df[col]), None).astype("object")
            df[col] = df[col].apply(lambda v: None if v is None else str(v))
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    # Write to JSONL temp file (avoids pandas/pyarrow version issues).
    # Sanitize: NaN → None (pandas serializes NaN as bare `NaN` which
    # isn't valid JSON), strip control chars from string fields.
    import json
    import re
    import math
    _CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
    def _clean(v):
        if v is None:
            return None
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                return None
            return v
        if isinstance(v, str):
            return _CTRL.sub("", v)
        return v
    rows = df.where(pd.notna(df), None).to_dict(orient="records")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False,
                                       encoding="utf-8") as f:
        for r in rows:
            cleaned = {k: _clean(v) for k, v in r.items()}
            f.write(json.dumps(cleaned, default=str, ensure_ascii=False) + "\n")
        tmp_path = f.name
    try:
        with open(tmp_path, "rb") as src:
            job = client.load_table_from_file(src, table_ref, job_config=job_config)
            job.result()  # wait
    finally:
        os.unlink(tmp_path)
    logger.info(f"bq_export: loaded {len(df)} rows → {table_ref}")
    return len(df)


def export_all(engine) -> dict:
    """Dump all configured tables to BigQuery. Returns counts."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT)
    except Exception as e:
        logger.warning(f"bq_export: BigQuery client init failed: {e}")
        return {}

    _ensure_dataset(client)
    counts = {}
    for tbl in TABLES:
        try:
            df = _table_to_dataframe(engine, tbl)
            counts[tbl] = _df_to_bigquery(client, df, tbl)
        except Exception as e:
            logger.warning(f"bq_export: {tbl} failed: {e}")
            counts[tbl] = -1
    logger.info(f"bq_export: done. counts={counts}")
    return counts
