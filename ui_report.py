"""Pulls jobs / stages / storage / executors of every app from the History Server REST API."""
import json
import urllib.request

API = "http://localhost:18080/api/v1"


def get(path):
    with urllib.request.urlopen(f"{API}/{path}") as r:
        return json.load(r)


def mb(n):
    return f"{n / 2**20:.1f} MiB"


for app in sorted(get("applications"), key=lambda a: a["attempts"][0]["startTimeEpoch"]):
    aid = app["id"]
    print(f"\n=== {app['name']}  ({aid})")
    stages = {s["stageId"]: s for s in get(f"applications/{aid}/stages")}
    for job in sorted(get(f"applications/{aid}/jobs"), key=lambda j: j["jobId"]):
        ins = [f"stage {sid}: {stages[sid]['numTasks']} tasks, input {mb(stages[sid]['inputBytes'])} "
               f"({stages[sid]['inputRecords']} rec), shuffle read {mb(stages[sid]['shuffleReadBytes'])}, "
               f"shuffle write {mb(stages[sid]['shuffleWriteBytes'])}"
               for sid in sorted(job["stageIds"]) if sid in stages and stages[sid]["status"] == "COMPLETE"]
        skipped = [sid for sid in job["stageIds"] if sid in stages and stages[sid]["status"] == "SKIPPED"]
        print(f"job {job['jobId']} [{job['status']}] '{job['name']}': tasks {job['numTasks']}"
              f" (completed {job['numCompletedTasks']}, skipped {job['numSkippedTasks']}, failed {job['numFailedTasks']})"
              + (f", skipped stages {skipped}" if skipped else ""))
        for line in ins:
            print("    " + line)
    for rdd in get(f"applications/{aid}/storage/rdd"):
        print(f"storage: RDD {rdd['id']} '{rdd['name']}' {rdd['storageLevel']}: "
              f"{rdd['numCachedPartitions']}/{rdd['numPartitions']} partitions, memory {mb(rdd['memoryUsed'])} "
              f"({rdd['memoryUsed']} B), disk {rdd['diskUsed']} B")
    for ex in get(f"applications/{aid}/allexecutors"):
        print(f"executor id={ex['id']} host={ex['hostPort']} cores={ex['totalCores']} tasks={ex['totalTasks']} "
              f"maxMemory={mb(ex['maxMemory'])}")
