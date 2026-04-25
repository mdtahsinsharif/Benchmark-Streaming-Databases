#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCHMARK_ENV_FILE="$SCRIPT_DIR/.benchmark_env.sh"

if [ -f "$BENCHMARK_ENV_FILE" ]; then
    # shellcheck disable=SC1090
    source "$BENCHMARK_ENV_FILE"
fi

ENGINE=""
SSH_USER="${USER:-}"
KAFKA_IP=""
KAFKA_PORT=""
BOOTSTRAP_SERVER=""
KAFKA_PARTITIONS=""
SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-localhost}"
SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"
SPARK_MASTER_URL=""
FLINK_MASTER_HOST="${JM_IP:-localhost}"
FLINK_MASTER_PORT="${JM_PORT:-6123}"

require_env() {
    local var_name="$1"
    if [ -z "${!var_name:-}" ]; then
        echo "Missing required environment variable: $var_name"
        echo "Run source $BENCHMARK_ENV_FILE first, or configure the cluster again."
        exit 1
    fi
}

require_kafka_cli_deps() {
    require_env "KAFKA_HOME"

    if ! find "$KAFKA_HOME/libs" -maxdepth 1 -name 'kafka-clients*.jar' | grep -q .; then
        echo "Kafka CLI dependency missing: kafka-clients jar not found in $KAFKA_HOME/libs"
        echo "Re-extract the Kafka archive and verify the installation before running start.sh."
        exit 1
    fi
}

prompt_inputs() {
    echo "-----------------------------------------------------------------"
    read -rp "Choose engine (flink/spark/both) [flink]: " ENGINE
    ENGINE="${ENGINE:-flink}"

    case "$ENGINE" in
        flink|spark|both) ;;
        *)
            echo "Invalid engine: $ENGINE"
            exit 1
            ;;
    esac

    read -rp "SSH user for worker hosts [$SSH_USER]: " input_ssh_user
    SSH_USER="${input_ssh_user:-$SSH_USER}"

    read -rp "Kafka broker IP [localhost]: " KAFKA_IP
    KAFKA_IP="${KAFKA_IP:-localhost}"

    read -rp "Kafka port [9092]: " KAFKA_PORT
    KAFKA_PORT="${KAFKA_PORT:-9092}"

    read -rp "Kafka partitions per topic [8]: " KAFKA_PARTITIONS
    KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-8}"

    BOOTSTRAP_SERVER="$KAFKA_IP:$KAFKA_PORT"
    SPARK_MASTER_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
}

ensure_directories() {
    [ -n "${SPARK_HOME:-}" ] && mkdir -p "$SPARK_HOME/logs" "$SPARK_HOME/work"
    [ -n "${FLINK_HOME:-}" ] && mkdir -p "$FLINK_HOME/log"
}

stop_processes() {
    echo "-----------------------------------------------------------------"
    echo "Stopping existing local processes..."

    pkill -f "flink" || true
    pkill -f "spark" || true
    pkill -f "kafka" || true
    pkill -f "nexmark" || true
    pkill -f "zookeeper" || true

    sleep 3
    echo "Existing local processes stopped."
}

configure_kafka() {
    require_env "KAFKA_HOME"
    require_kafka_cli_deps

    local config="$KAFKA_HOME/config/server.properties"

    echo "-----------------------------------------------------------------"
    echo "Configuring Kafka..."

    sed -i '/^advertised.listeners=/d' "$config"
    sed -i '/^listeners=/d' "$config"

    echo "listeners=PLAINTEXT://0.0.0.0:$KAFKA_PORT" >> "$config"
    echo "advertised.listeners=PLAINTEXT://$KAFKA_IP:$KAFKA_PORT" >> "$config"

    if ! grep -q '^transaction.max.timeout.ms=' "$config"; then
        echo "transaction.max.timeout.ms=900000" >> "$config"
    fi
}

start_zookeeper() {
    require_env "KAFKA_HOME"
    require_kafka_cli_deps

    echo "-----------------------------------------------------------------"
    echo "Starting Zookeeper..."
    "$KAFKA_HOME/bin/zookeeper-server-start.sh" \
        "$KAFKA_HOME/config/zookeeper.properties" \
        > "$KAFKA_HOME/zookeeper.log" 2>&1 &
    sleep 5
    echo "Zookeeper started."
}

start_kafka() {
    require_env "KAFKA_HOME"
    require_kafka_cli_deps

    echo "Starting Kafka..."
    "$KAFKA_HOME/bin/kafka-server-start.sh" \
        "$KAFKA_HOME/config/server.properties" \
        > "$KAFKA_HOME/kafka.log" 2>&1 &
    sleep 8
    echo "Kafka started."
}

reset_topics() {
    require_env "KAFKA_HOME"
    require_kafka_cli_deps

    echo "-----------------------------------------------------------------"
    echo "Resetting Kafka topics..."

    local existing_topics=""
    existing_topics=$("$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --list 2>/dev/null | grep -v '^__' || true)

    if [ -n "$existing_topics" ]; then
        local topic
        for topic in $existing_topics; do
            "$KAFKA_HOME/bin/kafka-topics.sh" \
                --delete \
                --topic "$topic" \
                --bootstrap-server "$BOOTSTRAP_SERVER" || true
        done
        sleep 3
    fi

    local topic
    for topic in nexmark data.ingestion flink.output spark.output; do
        "$KAFKA_HOME/bin/kafka-topics.sh" \
            --create \
            --if-not-exists \
            --topic "$topic" \
            --bootstrap-server "$BOOTSTRAP_SERVER" \
            --partitions "$KAFKA_PARTITIONS" \
            --replication-factor 1
    done

    echo "Kafka topics ready."
}

resolve_remote_home() {
    local engine="$1"
    local remote_home_var
    local remote_conf_var
    local local_home
    local local_conf

    if [ "$engine" = "flink" ]; then
        remote_home_var="FLINK_HOME"
        remote_conf_var="FLINK_CONF_DIR"
        local_home="${FLINK_HOME:-$REL_DIR/flink}"
        local_conf="${FLINK_CONF_DIR:-$local_home/conf}"
    else
        remote_home_var="SPARK_HOME"
        remote_conf_var="SPARK_CONF_DIR"
        local_home="${SPARK_HOME:-$REL_DIR/spark}"
        local_conf="${SPARK_CONF_DIR:-$local_home/conf}"
    fi

    cat <<EOF
if [ -n "\${$remote_home_var:-}" ] && [ -d "\${$remote_home_var}" ]; then
    home_dir="\${$remote_home_var}"
elif [ -d "$local_home" ]; then
    home_dir="$local_home"
elif [ -d "\$HOME/$(basename "$local_home")" ]; then
    home_dir="\$HOME/$(basename "$local_home")"
else
    echo "Could not find remote ${engine} home directory." >&2
    exit 1
fi

if [ -n "\${$remote_conf_var:-}" ] && [ -d "\${$remote_conf_var}" ]; then
    conf_dir="\${$remote_conf_var}"
elif [ -d "$local_conf" ]; then
    conf_dir="$local_conf"
else
    conf_dir="\$home_dir/conf"
fi
EOF
}

ssh_run() {
    local host="$1"
    local command="$2"

    if [ -z "$SSH_USER" ]; then
        echo "SSH user is empty. Re-run start.sh and provide an SSH user for worker hosts."
        exit 1
    fi

    ssh -n "$SSH_USER@$host" "$command"
}

read_workers() {
    local worker_file="$1"

    if [ ! -f "$worker_file" ]; then
        echo "Worker file not found: $worker_file"
        exit 1
    fi

    sed 's/[[:space:]]*$//' "$worker_file" \
        | sed '/^[[:space:]]*$/d' \
        | sed '/^[[:space:]]*#/d'
}

start_flink_workers() {
    require_env "FLINK_HOME"

    local worker_file="${FLINK_CONF_DIR:-$FLINK_HOME/conf}/workers"
    local -a workers=()
    local host
    mapfile -t workers < <(read_workers "$worker_file")

    for host in "${workers[@]}"; do
        echo "Starting remote Flink TaskManager on $host..."
        ssh_run "$host" "$(cat <<EOF
set -e
$(resolve_remote_home "flink")
mkdir -p "\$home_dir/log"
cd "\$home_dir"
"\$home_dir/bin/taskmanager.sh" start
EOF
)"
    done
}

start_spark_workers() {
    require_env "SPARK_HOME"

    local worker_file="${SPARK_CONF_DIR:-$SPARK_HOME/conf}/workers"
    local -a workers=()
    local host
    mapfile -t workers < <(read_workers "$worker_file")

    for host in "${workers[@]}"; do
        echo "Starting remote Spark worker on $host..."
        ssh_run "$host" "$(cat <<EOF
set -e
$(resolve_remote_home "spark")
mkdir -p "\$home_dir/logs" "\$home_dir/work"
cd "\$home_dir"
"\$home_dir/sbin/start-worker.sh" "$SPARK_MASTER_URL"
EOF
)"
    done
}

start_flink() {
    require_env "FLINK_HOME"

    echo "-----------------------------------------------------------------"
    echo "Starting Flink JobManager..."
    "$FLINK_HOME/bin/jobmanager.sh" start > "$FLINK_HOME/log/jobmanager.log" 2>&1 &
    sleep 3
    start_flink_workers
}

start_spark() {
    require_env "SPARK_HOME"

    echo "-----------------------------------------------------------------"
    echo "Starting Spark Master..."
    "$SPARK_HOME/sbin/start-master.sh" > "$SPARK_HOME/logs/master.log" 2>&1 &
    sleep 3
    start_spark_workers
}

show_summary() {
    echo "-----------------------------------------------------------------"
    echo "System startup complete"
    echo "Engine: $ENGINE"
    echo "Kafka: $BOOTSTRAP_SERVER"
    echo "Kafka partitions per topic: $KAFKA_PARTITIONS"

    if [ "$ENGINE" = "flink" ] || [ "$ENGINE" = "both" ]; then
        echo "Flink JobManager: $FLINK_MASTER_HOST:$FLINK_MASTER_PORT"
    fi

    if [ "$ENGINE" = "spark" ] || [ "$ENGINE" = "both" ]; then
        echo "Spark Master: $SPARK_MASTER_URL"
    fi

    echo "-----------------------------------------------------------------"
}

main() {
    prompt_inputs
    ensure_directories
    stop_processes
    configure_kafka
    start_zookeeper
    start_kafka
    reset_topics

    case "$ENGINE" in
        flink)
            start_flink
            ;;
        spark)
            start_spark
            ;;
        both)
            start_flink
            start_spark
            ;;
    esac

    sleep 2
    show_summary
}

main "$@"
