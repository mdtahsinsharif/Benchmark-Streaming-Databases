#!/bin/bash
set -euo pipefail

REL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <flink|spark|both> <ssh_user> [worker_file]"
    exit 1
fi

MODE="$1"
SSH_USER="$2"
case "$MODE" in
    flink|both)
        DEFAULT_WORKER_FILE="$REL_DIR/flink/conf/workers"
        ;;
    spark)
        DEFAULT_WORKER_FILE="$REL_DIR/spark/conf/workers"
        ;;
    *)
        echo "Unsupported mode: $MODE"
        exit 1
        ;;
esac

WORKER_FILE="${3:-$DEFAULT_WORKER_FILE}"

if [ ! -f "$WORKER_FILE" ]; then
    echo "Worker file not found: $WORKER_FILE"
    exit 1
fi

if { [ "$MODE" = "flink" ] || [ "$MODE" = "both" ]; } && [ ! -f "$REL_DIR/flink/conf/flink-conf-w.yaml" ]; then
    echo "Missing Flink worker template: $REL_DIR/flink/conf/flink-conf-w.yaml"
    exit 1
fi

if { [ "$MODE" = "flink" ] || [ "$MODE" = "both" ]; } && { [ ! -f "$REL_DIR/flink/conf/workers" ] || [ ! -f "$REL_DIR/flink/conf/masters" ]; }; then
    echo "Missing Flink topology files: $REL_DIR/flink/conf/workers and/or $REL_DIR/flink/conf/masters"
    exit 1
fi

if { [ "$MODE" = "spark" ] || [ "$MODE" = "both" ]; } && [ ! -f "$REL_DIR/spark/conf/spark-env-worker.sh.template" ]; then
    echo "Missing Spark worker template: $REL_DIR/spark/conf/spark-env-worker.sh.template"
    exit 1
fi

if { [ "$MODE" = "spark" ] || [ "$MODE" = "both" ]; } && [ ! -f "$REL_DIR/spark/conf/spark-defaults.conf" ]; then
    echo "Missing Spark defaults file: $REL_DIR/spark/conf/spark-defaults.conf"
    exit 1
fi

copy_flink_worker_conf() {
    local host="$1"
    local rendered_conf
    rendered_conf="$(mktemp)"

    sed "s#<IP_ADDRESS>#$host#g" "$REL_DIR/flink/conf/flink-conf-w.yaml" > "$rendered_conf"
    scp "$rendered_conf" "$SSH_USER@$host:/tmp/flink-conf.yaml"
    scp "$REL_DIR/flink/conf/workers" "$SSH_USER@$host:/tmp/flink-workers"
    scp "$REL_DIR/flink/conf/masters" "$SSH_USER@$host:/tmp/flink-masters"

    ssh "$SSH_USER@$host" '
set -e
if [ -n "${FLINK_HOME:-}" ] && [ -d "$FLINK_HOME/conf" ]; then
    conf_dir="$FLINK_HOME/conf"
elif [ -d "$HOME/UofT-HPRC_me/courses/Web-Scale/flink/conf" ]; then
    conf_dir="$HOME/UofT-HPRC_me/courses/Web-Scale/flink/conf"
elif [ -d "$HOME/flink/conf" ]; then
    conf_dir="$HOME/flink/conf"
else
    echo "Could not find remote Flink conf directory." >&2
    exit 1
fi

cp /tmp/flink-conf.yaml "$conf_dir/flink-conf.yaml"
cp /tmp/flink-workers "$conf_dir/workers"
cp /tmp/flink-masters "$conf_dir/masters"
echo "Updated remote Flink conf at: $conf_dir"
'

    rm -f "$rendered_conf"
}

copy_spark_worker_conf() {
    local host="$1"
    local rendered_env
    rendered_env="$(mktemp)"

    sed "s#<IP_ADDRESS>#$host#g" "$REL_DIR/spark/conf/spark-env-worker.sh.template" > "$rendered_env"

    scp "$rendered_env" "$SSH_USER@$host:/tmp/spark-env.sh"
    scp "$REL_DIR/spark/conf/spark-defaults.conf" "$SSH_USER@$host:/tmp/spark-defaults.conf"
    scp "$REL_DIR/spark/conf/workers" "$SSH_USER@$host:/tmp/spark-workers"

    ssh "$SSH_USER@$host" '
set -e
if [ -n "${SPARK_HOME:-}" ] && [ -d "$SPARK_HOME/conf" ]; then
    conf_dir="$SPARK_HOME/conf"
elif [ -d "$HOME/UofT-HPRC_me/courses/Web-Scale/spark/conf" ]; then
    conf_dir="$HOME/UofT-HPRC_me/courses/Web-Scale/spark/conf"
elif [ -d "$HOME/spark/conf" ]; then
    conf_dir="$HOME/spark/conf"
else
    echo "Could not find remote Spark conf directory." >&2
    exit 1
fi

mkdir -p "$conf_dir"
cp /tmp/spark-env.sh "$conf_dir/spark-env.sh"
cp /tmp/spark-defaults.conf "$conf_dir/spark-defaults.conf"
cp /tmp/spark-workers "$conf_dir/workers"
chmod +x "$conf_dir/spark-env.sh"
echo "Updated remote Spark conf at: $conf_dir"
'

    rm -f "$rendered_env"
}

mapfile -t WORKERS < <(
    sed 's/[[:space:]]*$//' "$WORKER_FILE" \
    | sed '/^[[:space:]]*$/d' \
    | sed '/^[[:space:]]*#/d' \
    | awk '!seen[$0]++'
)

if [ "${#WORKERS[@]}" -eq 0 ]; then
    echo "No worker hosts found in: $WORKER_FILE"
    exit 1
fi

for host in "${WORKERS[@]}"; do

    echo "Configuring worker: $host"

        case "$MODE" in
        flink)
            copy_flink_worker_conf "$host"
            ;;
        spark)
            copy_spark_worker_conf "$host"
            ;;
        both)
            copy_flink_worker_conf "$host"
            copy_spark_worker_conf "$host"
            ;;
        *)
            echo "Unsupported mode: $MODE"
            exit 1
            ;;
    esac

    echo "Worker $host prepared."
done
