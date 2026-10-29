#!/usr/bin/env bash
# Runs parts 1-3 one after another and saves the output to evidence/.
set -u
cd "$(dirname "$0")"
F='^ ?Container|NativeCodeLoader|incubator'
run() { docker compose run --rm --service-ports spark spark-submit "$@" 2>&1 | grep -v -E "$F"; }

docker compose up -d history
run my_part1.py | tee evidence/01_part1_output.txt
for m in lazy nocache cache; do run my_part2.py $m | tee evidence/02_part2_$m.txt; done
run my_part2.py bug > evidence/02_part2_bug.txt
for bs in 33554432 134217728; do
  for c in 1 2 4; do
    if [ $bs = 33554432 ]; then extra=(); else extra=(--conf spark.hadoop.fs.local.block.size=$bs); fi
    echo "\$ spark-submit --master \"local[$c]\" ${extra[*]} my_part3.py"
    run --master "local[$c]" "${extra[@]}" my_part3.py
  done
done | tee evidence/03_part3_output.txt
