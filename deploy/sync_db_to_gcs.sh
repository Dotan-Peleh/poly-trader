#!/bin/bash
# Sync the poly-trader SQLite DB to GCS every minute, namespaced under
# poly_live/ so it sits next to crypto-trader's data in the same bucket.
set -e
BOT_DIR="/home/dotanwork/poly-trader"
BUCKET="gs://crypto-trader-backups-494710"

sqlite3 "$BOT_DIR/poly_trader.db" ".backup /tmp/poly_snapshot.db"
gsutil -q -h "Cache-Control:no-cache" cp /tmp/poly_snapshot.db "$BUCKET/poly_live/poly.db"
rm /tmp/poly_snapshot.db
