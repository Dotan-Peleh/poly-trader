#!/usr/bin/env python3
"""Backfill strategy/source_wallet/decision_outcome/edge_definition
columns on historical `decisions` rows by regex-parsing `notes`.

Run once after deploying the 2026-05-19 observability migration.
Idempotent — only fills NULL columns, never overwrites populated ones.

  python3 scripts/backfill_decision_columns.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.orm import Session

from data.storage import engine, Decision, init_db

# Regex catalog matches the patterns observed in production decisions.notes:
#   smart_copy:<label>|<0xWALLET>...                                  → smart_copy
#   early_exit:smart_exit:<label>_100pct|smart_copy:...|0xWALLET      → smart_copy + exit_copy_signal
#   early_exit:sl_<X>x|v2_meanrev                                     → v2_meanrev + exit_stop_loss
#   early_exit:tp_<X>x|v2_meanrev                                     → v2_meanrev + exit_take_profit
#   early_exit:time_bailout_<sec>s|v2_meanrev                         → v2_meanrev + exit_time_bailout
#   v2_meanrev                                                        → v2_meanrev + held_to_expiry
#   v2_meanrev|region_blocked_phantom                                 → v2_meanrev + region_blocked
RE_SMART_WALLET = re.compile(r"smart_copy:[^|]+\|(0x[0-9a-fA-F]+)")
RE_EXIT_SL = re.compile(r"early_exit:sl_")
RE_EXIT_TP = re.compile(r"early_exit:tp_")
RE_EXIT_TB = re.compile(r"early_exit:time_bailout_")
RE_EXIT_SMART = re.compile(r"early_exit:smart_exit:")
RE_REGION = re.compile(r"region_blocked_phantom")


def classify(notes: str | None) -> dict:
    n = notes or ""
    out: dict = {}
    if "smart_copy" in n or "smart_exit" in n:
        out["strategy"] = "smart_copy"
        out["edge_definition"] = "smart_copy_follow"
        m = RE_SMART_WALLET.search(n)
        if m:
            out["source_wallet"] = m.group(1)
    else:
        out["strategy"] = "v2_meanrev"
        out["edge_definition"] = "model_minus_implied"
    if RE_EXIT_SMART.search(n):
        out["decision_outcome"] = "exit_copy_signal"
    elif RE_EXIT_SL.search(n):
        out["decision_outcome"] = "exit_stop_loss"
    elif RE_EXIT_TP.search(n):
        out["decision_outcome"] = "exit_take_profit"
    elif RE_EXIT_TB.search(n):
        out["decision_outcome"] = "exit_time_bailout"
    elif RE_REGION.search(n):
        out["decision_outcome"] = "region_blocked_phantom"
    else:
        out["decision_outcome"] = "held_to_expiry"
    return out


def main():
    init_db()
    updated = 0
    with Session(engine) as session:
        rows = (session.query(Decision)
                .filter((Decision.strategy.is_(None)) |
                        (Decision.decision_outcome.is_(None)))
                .all())
        print(f"backfill: {len(rows)} rows need classification")
        for d in rows:
            patch = classify(d.notes)
            for k, v in patch.items():
                setattr(d, k, v)
            updated += 1
        session.commit()
    print(f"backfill: updated {updated} rows")

    # Distribution check
    from sqlalchemy import func
    with Session(engine) as session:
        dist = (session.query(Decision.strategy, func.count(Decision.id))
                .group_by(Decision.strategy).all())
        print(f"distribution: {dict(dist)}")
        outc = (session.query(Decision.decision_outcome, func.count(Decision.id))
                .group_by(Decision.decision_outcome).all())
        print(f"outcomes: {dict(outc)}")


if __name__ == "__main__":
    main()
