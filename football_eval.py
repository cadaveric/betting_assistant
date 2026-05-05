#!/usr/bin/env python3
"""Offline football prediction audit for Scoutline.

This evaluates the stored prediction ledger. It does not claim a full backtest:
it measures the predictions the app actually saved and graded.
"""
import json
import sys

import proxy


def main():
    rows = proxy._load_predictions()
    audit = proxy._football_audit_summary(rows)
    if "--json" in sys.argv:
        print(json.dumps(audit, indent=2))
        return

    summary = audit.get("summary") or {}
    print("Football prediction audit")
    print("=========================")
    print(f"predictions: {audit['footballPredictions']}")
    print(f"graded:      {audit['graded']}")
    print(f"pending:     {audit['pending']}")
    print(f"accuracy:    {summary.get('outcomeAccuracy')}%")
    print(f"avg brier:   {summary.get('avgBrier')}")
    print()
    for group_name, group in (audit.get("groups") or {}).items():
        print(group_name)
        for key, row in group.items():
            print(
                f"  {key:14} n={row['n']:3} "
                f"acc={row['accuracy']}% brier={row['avgBrier']} "
                f"o25={row['over25Accuracy']}%/{row['over25N']}"
            )
        print()


if __name__ == "__main__":
    main()
