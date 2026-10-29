"""HW5, part 1: HW4 (daily totals per category + leading category) as one RDD chain.

Run:  docker compose run --rm --service-ports spark spark-submit my_part1.py
"""
import statistics
import time
from operator import add

from pyspark import SparkContext

PATH = "file:///work/data/transactions.csv"
REPEATS = 3


def parse(line):
    # "user_id,timestamp,category,amount" -> ((date, category), amount in cents)
    user_id, ts, category, amount = line.split(",")
    return (ts[:10], category), round(float(amount) * 100)


def pick_leader(a, b):
    # a, b = (category sum, category, Task 1 records); keep the bigger sum, count all records
    best = a if (a[0], a[1]) >= (b[0], b[1]) else b
    return best[0], best[1], a[2] + b[2]


def build_chain(sc):
    lines = sc.textFile(PATH)
    pairs = lines.filter(lambda s: not s.startswith("user_id")).map(parse)
    daily = pairs.reduceByKey(add)                                   # Task 1: (date, category) -> sum
    totals = (daily.map(lambda kv: (kv[0][1], (kv[1], 1)))
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))  # Task 2: category -> (sum, records)
    leader = totals.map(lambda kv: (kv[1][0], kv[0], kv[1][1]))
    return lines, leader


def main():
    sc = SparkContext(appName="hw5-part1 rdd-chain")
    print(f"app: {sc.appName}  id: {sc.applicationId}  master: {sc.master}  file: {PATH}")

    lines, leader = build_chain(sc)
    print(f"textFile partitions: {lines.getNumPartitions()}")

    times = []
    for run in range(REPEATS + 1):
        lines, leader = build_chain(sc)          # rebuilt from textFile every time: no shuffle reuse
        t0 = time.perf_counter()
        total, category, records = leader.reduce(pick_leader)
        dt = time.perf_counter() - t0
        label = "warm-up" if run == 0 else f"run {run}"
        print(f"{label}: action reduce() took {dt:.3f} s")
        if run > 0:
            times.append(dt)

    print(f"times (s): {', '.join(f'{t:.3f}' for t in times)}; median: {statistics.median(times):.3f}")
    print(f"leader: {category}  sum: {total // 100}.{total % 100:02d}  Task 1 records: {records}")
    print("lineage of the Task 2 RDD:")
    print(leader.toDebugString().decode())
    sc.stop()


if __name__ == "__main__":
    main()
