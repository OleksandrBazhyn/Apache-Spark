"""Helper for report question 2: follows file lines 100-102 through parse() into the Task 1 result."""
from operator import add

from pyspark import SparkContext

from my_part1 import PATH, parse

FIRST, LAST = 100, 102          # 1-based line numbers in the file (line 1 is the header)

sc = SparkContext(appName="hw5-q2 trace lines 100-102")
lines = sc.textFile(PATH)
picked = lines.zipWithIndex().filter(lambda li: FIRST - 1 <= li[1] <= LAST - 1).sortBy(lambda li: li[1]).collect()
keys = set()
for line, idx in picked:
    key, cents = parse(line)
    keys.add(key)
    print(f"line {idx + 1}: {line!r} -> parse() = ({key!r}, {cents})")

daily = lines.filter(lambda s: not s.startswith("user_id")).map(parse).reduceByKey(add)
for key, total in sorted(daily.filter(lambda kv: kv[0] in keys).collect()):
    print(f"Task 1 record {key}: {total} cents = {total // 100}.{total % 100:02d}")
print(f"Task 1 records total: {daily.count()}")
totals = daily.map(lambda kv: (kv[0][1], kv[1])).reduceByKey(add).sortBy(lambda kv: -kv[1]).collect()
for cat, total in totals:
    print(f"category {cat:<14} {total // 100}.{total % 100:02d}")
sc.stop()
