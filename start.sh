#!/bin/bash
set -e

# -----------------------------
# CONFIG INPUT
# -----------------------------
echo "-----------------------------------------------------------------"
read -p "Choose engine (flink/spark/all) [default: flink]: " ENGINE
ENGINE=${ENGINE:-flink}

read -p "Is this node a Manager or worker? (m/w): " NODE_ROLE

# Kafka
read -p "Enter Kafka broker IP (default: localhost): " KAFKA_IP
KAFKA_IP=${KAFKA_IP:-localhost}

read -p "Enter Kafka port (default: 9092): " KAFKA_PORT
KAFKA_PORT=${KAFKA_PORT:-9092}

BOOTSTRAP_SERVER="$KAFKA_IP:$KAFKA_PORT"

# -----------------------------
# STOP EXISTING PROCESSES
# -----------------------------
echo "-----------------------------------------------------------------"
echo "Stopping existing processes..."

pkill -f "flink" || true
pkill -f "spark" || true
pkill -f "kafka" || true
pkill -f "zookeeper" || true

sleep 3
echo "Existing processes stopped."

# -----------------------------
# START ZOOKEEPER (M ONLY)
# -----------------------------
echo "-----------------------------------------------------------------"
if [[ "$NODE_ROLE" == "m" ]]; then
    echo "Starting Zookeeper..."

    "$KAFKA_HOME/bin/zookeeper-server-start.sh" \
        "$KAFKA_HOME/config/zookeeper.properties" \
        > "$KAFKA_HOME/zookeeper.log" 2>&1 &

    sleep 5
    echo "Zookeeper started."
fi

# -----------------------------
# START KAFKA (M ONLY)
# -----------------------------
if [[ "$NODE_ROLE" == "m" ]]; then
    echo "Configuring Kafka..."

    CONFIG="$KAFKA_HOME/config/server.properties"

    sed -i '/^advertised.listeners=/d' "$CONFIG"
    sed -i '/^listeners=/d' "$CONFIG"

    echo "listeners=PLAINTEXT://0.0.0.0:$KAFKA_PORT" >> "$CONFIG"
    echo "advertised.listeners=PLAINTEXT://$KAFKA_IP:$KAFKA_PORT" >> "$CONFIG"

    if ! grep -q "^transaction.max.timeout.ms=" "$CONFIG"; then
        echo "transaction.max.timeout.ms=900000" >> "$CONFIG"
    fi

    echo "Starting Kafka..."
    "$KAFKA_HOME/bin/kafka-server-start.sh" \
        "$CONFIG" \
        > "$KAFKA_HOME/kafka.log" 2>&1 &

    sleep 8
    echo "Kafka started."
fi

# -----------------------------
# RESET KAFKA TOPICS (M ONLY)
# -----------------------------
echo "-----------------------------------------------------------------"
if [[ "$NODE_ROLE" == "m" ]]; then
    echo "Setting up Kafka topics..."

    EXISTING_TOPICS=$("$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --list 2>/dev/null | grep -v "^__" || true)

    if [ -n "$EXISTING_TOPICS" ]; then
        echo "Deleting topics..."
        for topic in $EXISTING_TOPICS; do
            "$KAFKA_HOME/bin/kafka-topics.sh" \
                --delete \
                --topic "$topic" \
                --bootstrap-server "$BOOTSTRAP_SERVER" || true
        done
    fi

    for topic in data.ingestion flink.output spark.output; do
        "$KAFKA_HOME/bin/kafka-topics.sh" \
            --create \
            --topic "$topic" \
            --bootstrap-server "$BOOTSTRAP_SERVER" \
            --partitions 3 \
            --replication-factor 1 || true
    done

    echo "Kafka topics ready."
fi

# -----------------------------
# ENSURE DIRECTORIES EXIST
# -----------------------------
mkdir -p "$SPARK_HOME/logs"
mkdir -p "$FLINK_HOME/log"

# -----------------------------
# SPARK: Prompt for Master Info (ALL NODES)
# -----------------------------
if [[ "$ENGINE" == "spark" || "$ENGINE" == "all" ]]; then
    read -p "Enter Spark Master IP (default: localhost): " MASTER_IP
    MASTER_IP=${MASTER_IP:-localhost}

    read -p "Enter Spark Master port (default: 7077): " MASTER_PORT
    MASTER_PORT=${MASTER_PORT:-7077}

    MASTER_URL="spark://$MASTER_IP:$MASTER_PORT"
fi

# -----------------------------
# START ENGINE
# -----------------------------
echo "-----------------------------------------------------------------"

# Function to start Flink
start_flink() {
    echo "Starting Flink..."
    if [[ "$NODE_ROLE" == "m" ]]; then
        "$FLINK_HOME/bin/start-cluster.sh" > "$FLINK_HOME/log/cluster.log" 2>&1 &
    else
        "$FLINK_HOME/bin/taskmanager.sh" start > "$FLINK_HOME/log/taskmanager.log" 2>&1 &
    fi
}

# Function to start Spark
start_spark() {
    echo "Starting Spark..."
    if [[ "$NODE_ROLE" == "m" ]]; then
        "$SPARK_HOME/sbin/start-master.sh" > "$SPARK_HOME/logs/master.log" 2>&1 &
        sleep 3
        echo "You can now start workers on this or other nodes."
    else
        "$SPARK_HOME/sbin/start-worker.sh" "$MASTER_URL" > "$SPARK_HOME/logs/worker.log" 2>&1 &
    fi
}

if [[ "$ENGINE" == "flink" ]]; then
    start_flink
elif [[ "$ENGINE" == "spark" ]]; then
    start_spark
elif [[ "$ENGINE" == "all" ]]; then
    start_flink
    start_spark
else
    echo "Invalid engine: $ENGINE"
    exit 1
fi

sleep 2

# -----------------------------
# FINAL OUTPUT
# -----------------------------
echo "-----------------------------------------------------------------"
echo "✅ System startup complete"
echo "Engine: $ENGINE"
echo "Kafka: $BOOTSTRAP_SERVER"

if [[ "$ENGINE" == "flink" || "$ENGINE" == "all" ]]; then
    echo "Flink JobManager: $JM_IP:$JM_PORT"
    echo "Flink UI: http://$JM_IP:8081"
fi

if [[ "$ENGINE" == "spark" || "$ENGINE" == "all" ]]; then
    echo "Spark Master: spark://$MASTER_IP:$MASTER_PORT"
    echo "Spark UI: http://$MASTER_IP:8080"
fi

echo "-----------------------------------------------------------------"