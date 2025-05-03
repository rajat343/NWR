#!/bin/bash
MAX_TASKS=100
echo 0 > global_counter.txt

for i in {0..4}
do
    port=$((50050 + i))
    python3 server.py --id $i --port $port --max_tasks 100 &
done