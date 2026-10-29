#!/usr/bin/env python3
"""Transaction log generator in CSV format: user_id,timestamp,category,amount.

Same generator as in HW4 (fixed seed => deterministic output), only the default
volume is increased: 6 000 000 rows instead of 800 000.
"""

import argparse
import random
from datetime import datetime, timedelta

# Category -> (min amount, max amount)
CATEGORIES = {
    "groceries": (2.0, 150.0),
    "electronics": (15.0, 2500.0),
    "books": (5.0, 80.0),
    "clothing": (10.0, 400.0),
    "restaurants": (5.0, 200.0),
    "transport": (1.5, 120.0),
    "entertainment": (5.0, 250.0),
    "health": (3.0, 500.0),
    "utilities": (20.0, 300.0),
    "travel": (50.0, 3000.0),
}
# Weights so that purchase frequency differs between categories
WEIGHTS = [30, 4, 6, 8, 18, 15, 7, 5, 3, 4]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", "--output", default="data/transactions.csv")
    parser.add_argument("-n", "--rows", type=int, default=6_000_000)
    parser.add_argument("--users", type=int, default=5_000, help="number of unique users")
    parser.add_argument("--start", default="2026-01-01", help="start date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=365, help="period length in days")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    start = datetime.fromisoformat(args.start)
    span_minutes = args.days * 24 * 60
    names = list(CATEGORIES)
    user_ids = list(range(1000, 1000 + args.users))

    with open(args.output, "w", encoding="utf-8", newline="") as f:
        f.write("user_id,timestamp,category,amount\n")
        batch = []
        for _ in range(args.rows):
            category = rng.choices(names, WEIGHTS)[0]
            lo, hi = CATEGORIES[category]
            amount = round(rng.uniform(lo, hi), 2)
            if amount <= 0:
                amount = lo
            ts = start + timedelta(minutes=rng.randrange(span_minutes))
            batch.append(f"{rng.choice(user_ids)},{ts:%Y-%m-%dT%H:%M:%S},{category},{amount:.2f}\n")
            if len(batch) >= 10_000:
                f.writelines(batch)
                batch.clear()
        f.writelines(batch)

    print(f"Generated {args.rows} rows in {args.output}")


if __name__ == "__main__":
    main()
