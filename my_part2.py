"""HW5, part 2: lazy evaluation and repeated reading of the file.

Run one mode per application (each one is a separate app in the History Server):
  spark-submit my_part2.py lazy     # 2.1: time to build the chain vs. time of the action
  spark-submit my_part2.py bug      # 2.2: deliberate bug in the parse function
  spark-submit my_part2.py nocache  # 2.3: two actions in a row over the parsed pairs, no cache
  spark-submit my_part2.py cache    # 2.4: the same after cache()
"""
import os
import statistics
import sys
import time
from operator import add

from pyspark import SparkContext

PATH = "file:///work/data/transactions.csv"
REPEATS = 3


def parse(line):
    user_id, ts, category, amount = line.split(",")
    return (ts[:10], category), round(float(amount) * 100)


def parse_bad(line):
    user_id, ts, category, amount = line.split(",")
    return (ts[:10], category), round(float(category) * 100)   # BUG: category is not a number


def pick_leader(a, b):
    best = a if (a[0], a[1]) >= (b[0], b[1]) else b
    return best[0], best[1], a[2] + b[2]


def build_pairs(sc, parse_fn=parse):
    lines = sc.textFile(PATH)
    return lines.filter(lambda s: not s.startswith("user_id")).map(parse_fn)


def build_chain(sc, parse_fn=parse):
    pairs = build_pairs(sc, parse_fn)
    daily = pairs.reduceByKey(add)
    totals = (daily.map(lambda kv: (kv[0][1], (kv[1], 1)))
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
    return totals.map(lambda kv: (kv[1][0], kv[0], kv[1][1]))


def report(name, times):
    print(f"{name}: {', '.join(f'{t:.4f}' for t in times)} s; median {statistics.median(times):.4f} s")


def run_lazy(sc):
    build_times, action_times = [], []
    for run in range(REPEATS + 1):
        t0 = time.perf_counter()
        leader = build_chain(sc)
        t1 = time.perf_counter()
        result = leader.reduce(pick_leader)
        t2 = time.perf_counter()
        print(f"{'warm-up' if run == 0 else f'run {run}'}: build {t1 - t0:.4f} s, action {t2 - t1:.3f} s -> {result}")
        if run > 0:
            build_times.append(t1 - t0)
            action_times.append(t2 - t1)
    report("build chain (textFile .. last map)", build_times)
    report("action reduce()", action_times)


def run_bug(sc):
    leader = build_chain(sc, parse_bad)
    print("chain with parse_bad built without errors, calling the action...")
    result = leader.reduce(pick_leader)
    print(result)


def action_pair(pairs):
    t0 = time.perf_counter()
    n_pairs = pairs.count()
    n_daily = pairs.reduceByKey(add).count()
    dt = time.perf_counter() - t0
    return dt, n_pairs, n_daily


def run_nocache(sc):
    times = []
    for run in range(REPEATS + 1):
        pairs = build_pairs(sc)
        dt, n_pairs, n_daily = action_pair(pairs)
        print(f"{'warm-up' if run == 0 else f'run {run}'}: pair of actions {dt:.3f} s (count={n_pairs}, daily={n_daily})")
        if run > 0:
            times.append(dt)
    report("pair of actions without cache", times)


def run_cache(sc):
    warm = build_pairs(sc)
    dt, _, _ = action_pair(warm)
    print(f"warm-up (no cache): pair of actions {dt:.3f} s")

    first, following = [], []
    for run in range(1, REPEATS + 1):
        pairs = build_pairs(sc).cache()
        for i in range(REPEATS + 1):
            dt, n_pairs, n_daily = action_pair(pairs)
            print(f"cache run {run}, pair {i + 1}: {dt:.3f} s (count={n_pairs}, daily={n_daily})")
            (first if i == 0 else following).append(dt)
        if run < REPEATS:
            pairs.unpersist(blocking=True)      # the last cached RDD stays for the Storage tab
    report("first pair after cache()", first)
    report("following pairs (from cache)", following)
    for info in sc._jsc.sc().getRDDStorageInfo():
        print(f"Storage: RDD {info.id()} '{info.name()}' {info.numCachedPartitions()}/{info.numPartitions()} "
              f"partitions cached, memory {info.memSize()} B ({info.memSize() / 2**20:.1f} MiB), disk {info.diskSize()} B")
    time.sleep(int(os.environ.get("HOLD_UI", "0")))   # keep the live UI (port 4040) open for the Storage tab


def main():
    mode = sys.argv[1]
    sc = SparkContext(appName=f"hw5-part2 {mode}")
    print(f"app: {sc.appName}  id: {sc.applicationId}  master: {sc.master}  file: {PATH}")
    {"lazy": run_lazy, "bug": run_bug, "nocache": run_nocache, "cache": run_cache}[mode](sc)
    sc.stop()


if __name__ == "__main__":
    main()
