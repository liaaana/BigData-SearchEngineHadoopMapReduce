#!/bin/bash
INPUT=${1:-/index/data/}
OUTPUT="/tmp/index"

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -archives /app/.venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper1.py" \
  -reducer ".venv/bin/python reducer1.py" \
  -input "$INPUT" \
  -output "$OUTPUT" 
