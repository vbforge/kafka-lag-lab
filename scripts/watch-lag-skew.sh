#!/usr/bin/env bash
# kafka-lag-lab :: watch-lag-skew.sh
#
# JUNIOR NOTE: same underlying data as watch-lag.sh (kafka-consumer-groups
# --describe), but reduced to one number: how much worse is the WORST
# partition than the AVERAGE partition. Near 1.0x = healthy/even.
# Climbing into the 5x/10x+ range = a hot-partition problem.

GROUP="${1:-lag-lab-group}"

echo "Watching lag skew for group '$GROUP' (Ctrl+C to stop)..."
echo ""

while true; do
    clear
    echo "=== kafka-lag-lab :: skew check :: $(date '+%H:%M:%S') ==="
    echo ""

    RAW=$(docker exec kafka-lag-lab-kafka \
        kafka-consumer-groups --bootstrap-server localhost:19092 \
        --describe --group "$GROUP" 2>/dev/null | tail -n +2 | grep -v '^$')

    echo "$RAW" | awk '{printf "partition=%-3s lag=%-6s\n", $3, $6}'
    echo ""

    echo "$RAW" | awk '
        {
            lag = $6 + 0
            sum += lag
            n++
            if (lag > max) { max = lag; maxp = $3 }
        }
        END {
            if (n == 0) { print "no data yet"; exit }
            avg = sum / n
            printf "avg lag = %.1f | max lag = %d on partition %s\n", avg, max, maxp
            if (avg > 0) {
                printf "skew ratio (max/avg) = %.1fx\n", max / avg
            } else {
                print "skew ratio = n/a (avg is 0)"
            }
        }
    '
    sleep 2
done