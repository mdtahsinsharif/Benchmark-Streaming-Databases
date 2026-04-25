#!/bin/bash

MASTER="spark://10.1.2.104:7077"
DRIVER_HOST="10.1.2.104"

BASE_RATE=10000 # 200000   # Q11    10000 Spark 
TRIGGER="1 second"
DURATION=60

BASE_OUTPUT="/srv/nfs/spark/nexmark/scalability"
Q="q11"

SPARK_MASTER_UI="http://10.1.2.104:8080"

# ---------------- EXPERIMENT SPACE ----------------
SIZES=(1 5 10 15 20 30)

echo "======================================"
echo "Running scalability sweep for $Q"
echo "CORES = PARTITIONS = ${SIZES[*]}"
echo "======================================"

for SIZE in "${SIZES[@]}"; do

    CORES=$SIZE
    PARTITIONS=$SIZE
    TPS=$((BASE_RATE * CORES)) 

    echo ""
    echo "======================================"
    echo "RUNNING: SIZE=$SIZE (CORES=$CORES, PARTITIONS=$PARTITIONS)"
    echo "======================================"

    OUTPUT_PATH="${BASE_OUTPUT}/${Q}/p${PARTITIONS}"
    mkdir -p "$OUTPUT_PATH"

    LOG_FILE="/tmp/${Q}_p${PARTITIONS}.log"

    # ---------------- START SPARK JOB ----------------
    spark-submit \
      --master $MASTER \
      --conf spark.driver.host=$DRIVER_HOST \
      --total-executor-cores $CORES \
      --conf spark.sql.shuffle.partitions=$PARTITIONS \
      --conf spark.default.parallelism=$PARTITIONS \
      scripts/queries/${Q}.py \
      --rows-per-second $TPS \
      --num-partitions $PARTITIONS \
      --trigger-processing-time "$TRIGGER" \
      --sink-format csv \
      --output-path "$OUTPUT_PATH" \
      > "$LOG_FILE" 2>&1 &

    DRIVER_PID=$!
    echo "Started $Q (SIZE=$SIZE) PID=$DRIVER_PID"

    # ---------------- WAIT FOR APP ID ----------------
    echo "Waiting for Spark application ID..."

    APP_ID=""
    for i in {1..30}; do
        APP_ID=$(grep -oE 'app-[0-9\-]+' "$LOG_FILE" | head -n 1)
        if [ ! -z "$APP_ID" ]; then
            break
        fi
        sleep 2
    done

    echo "Detected APP_ID: $APP_ID"

    # ---------------- RUN ----------------
    sleep $DURATION

    echo "Stopping SIZE=$SIZE"

    # ---------------- GRACEFUL STOP ----------------
    kill -TERM $DRIVER_PID 2>/dev/null
    sleep 10

    # ---------------- STOP SPARK APP ----------------
    if [ ! -z "$APP_ID" ]; then
        echo "Killing Spark app $APP_ID"
        curl -X POST \
            "$SPARK_MASTER_UI/api/v1/applications/$APP_ID/kill" \
            >/dev/null 2>&1
    else
        echo "WARNING: APP_ID not found"
    fi

    sleep 5

    # ---------------- FORCE CLEANUP ----------------
    if ps -p $DRIVER_PID > /dev/null; then
        echo "Force killing driver"
        kill -9 $DRIVER_PID
    fi

    echo "Finished SIZE=$SIZE"
    echo ""

done

echo "======================================"
echo "Scalability sweep completed"
echo "======================================"