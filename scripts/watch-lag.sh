#!/usr/bin/env bash
# kafka-lag-lab :: watch-lag.sh
#
# JUNIOR NOTE: this is Phase 1 of the observability plan (simple/scriptable) —
# polls Kafka's own consumer-group tool every 2 seconds and prints lag
# per partition. Ctrl+C to stop. We'll layer Grafana on top of this
# same data later without throwing this away.

GROUP="${1:-lag-lab-group}"

echo "Watching lag for group '$GROUP' (Ctrl+C to stop)..."
echo ""

while true; do
    clear
    echo "=== kafka-lag-lab :: consumer lag :: $(date '+%H:%M:%S') ==="
    docker exec kafka-lag-lab-kafka \
        kafka-consumer-groups --bootstrap-server localhost:19092 \
        --describe --group "$GROUP"
    sleep 2
done