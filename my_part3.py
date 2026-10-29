"""HW5, part 3: the action from part 1 under different cores and split sizes.

The code does not change between configurations, only spark-submit parameters:
  spark-submit --master "local[1]" my_part3.py
  spark-submit --master "local[4]" --conf spark.hadoop.fs.local.block.size=134217728 my_part3.py
"""
import statistics
import time
from operator import add

from pyspark import SparkConf, SparkContext

PATH = "file:///work/data/transactions.csv"
REPEATS = 3


def parse(line):
    user_id, ts, category, amount = line.split(",")
    return (ts[:10], category), round(float(amount) * 100)


def pick_leader(a, b):
    best = a if (a[0], a[1]) >= (b[0], b[1]) else b
    return best[0], best[1], a[2] + b[2]


def build_chain(sc):
    lines = sc.textFile(PATH)
    pairs = lines.filter(lambda s: not s.startswith("user_id")).map(parse)
    daily = pairs.reduceByKey(add)
    totals = (daily.map(lambda kv: (kv[0][1], (kv[1], 1)))
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
    leader = totals.map(lambda kv: (kv[1][0], kv[0], kv[1][1]))
    return lines, leader


def main():
    SparkContext._ensure_initialized()          # start the JVM so SparkConf sees the spark-submit parameters
    conf = SparkConf()
    master = conf.get("spark.master")
    block = int(conf.get("spark.hadoop.fs.local.block.size", str(32 * 1024 * 1024)))
    sc = SparkContext(conf=conf.setAppName(f"hw5-part3 {master} block={block // 2**20}MB"))
    tracker = sc.statusTracker()
    print(f"app: {sc.appName}  id: {sc.applicationId}  file: {PATH}")
    print(f"master: {master}  defaultParallelism: {sc.defaultParallelism}  fs.local.block.size: {block}")

    times = []
    for run in range(REPEATS + 1):
        lines, leader = build_chain(sc)
        seen = set(tracker.getJobIdsForGroup())
        t0 = time.perf_counter()
        leader.reduce(pick_leader)
        dt = time.perf_counter() - t0
        for job_id in sorted(set(tracker.getJobIdsForGroup()) - seen):
            stages = [tracker.getStageInfo(s) for s in tracker.getJobInfo(job_id).stageIds]
            tasks = sum(s.numTasks for s in stages if s)
            per_stage = ", ".join(f"stage {s.stageId}: {s.numTasks}" for s in sorted(stages, key=lambda s: s.stageId) if s)
            print(f"{'warm-up' if run == 0 else f'run {run}'}: partitions {lines.getNumPartitions()}, "
                  f"job {job_id}: {tasks} tasks ({per_stage}), time {dt:.3f} s")
        if run > 0:
            times.append(dt)

    print(f"RESULT master={master} block={block // 2**20}MB partitions={lines.getNumPartitions()} "
          f"times={','.join(f'{t:.3f}' for t in times)} median={statistics.median(times):.3f}")
    sc.stop()


if __name__ == "__main__":
    main()
