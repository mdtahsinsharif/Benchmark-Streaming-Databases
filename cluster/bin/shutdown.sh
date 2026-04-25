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

require_env() {
    local var_name="$1"
    if [ -z "${!var_name:-}" ]; then
        echo "Missing required environment variable: $var_name"
        echo "Run source $BENCHMARK_ENV_FILE first, or configure the cluster again."
        exit 1
    fi
}

prompt_inputs() {
    echo "-----------------------------------------------------------------"
    read -rp "Choose engine to shut down (flink/spark/both) [flink]: " ENGINE
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
}

resolve_remote_home() {
    local engine="$1"
    local remote_home_var
    local local_home

    if [ "$engine" = "flink" ]; then
        remote_home_var="FLINK_HOME"
        local_home="${FLINK_HOME:-$REL_DIR/flink}"
    else
        remote_home_var="SPARK_HOME"
        local_home="${SPARK_HOME:-$REL_DIR/spark}"
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
EOF
}

ssh_run() {
    local host="$1"
    local command="$2"

    if [ -z "$SSH_USER" ]; then
        echo "SSH user is empty. Re-run shutdown.sh and provide an SSH user for worker hosts."
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

shutdown_flink_workers() {
    require_env "FLINK_HOME"

    local worker_file="${FLINK_CONF_DIR:-$FLINK_HOME/conf}/workers"
    local -a workers=()
    local host
    mapfile -t workers < <(read_workers "$worker_file")

    for host in "${workers[@]}"; do
        echo "Stopping remote Flink TaskManagers on $host..."
        if ! ssh_run "$host" "$(cat <<EOF
set -e
$(resolve_remote_home "flink")
pkill -f 'org.apache.flink.runtime.taskexecutor.TaskManagerRunner' || true
pkill -f 'flink-daemon.sh.*taskmanager' || true
if [ -x "\$home_dir/bin/taskmanager.sh" ]; then
    "\$home_dir/bin/taskmanager.sh" stop || true
fi
EOF
)"
        then
            echo "Warning: failed to fully stop Flink workers on $host"
        fi
    done
}

shutdown_spark_workers() {
    require_env "SPARK_HOME"

    local worker_file="${SPARK_CONF_DIR:-$SPARK_HOME/conf}/workers"
    local -a workers=()
    local host
    mapfile -t workers < <(read_workers "$worker_file")

    for host in "${workers[@]}"; do
        echo "Stopping remote Spark workers on $host..."
        if ! ssh_run "$host" "$(cat <<EOF
set -e
$(resolve_remote_home "spark")
if [ -x "\$home_dir/sbin/stop-worker.sh" ]; then
    "\$home_dir/sbin/stop-worker.sh" || true
fi
pkill -f 'org.apache.spark.deploy.worker.Worker' || true
sleep 2
pkill -9 -f 'org.apache.spark.deploy.worker.Worker' || true
EOF
)"
        then
            echo "Warning: failed to fully stop Spark workers on $host"
        fi
    done
}

shutdown_flink() {
    require_env "FLINK_HOME"

    echo "-----------------------------------------------------------------"
    echo "Stopping Flink..."
    shutdown_flink_workers
    "$FLINK_HOME/bin/jobmanager.sh" stop || true
    pkill -f 'org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint' || true
    pkill -f 'org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint' || true
}

shutdown_spark() {
    require_env "SPARK_HOME"

    echo "-----------------------------------------------------------------"
    echo "Stopping Spark..."
    shutdown_spark_workers
    "$SPARK_HOME/sbin/stop-master.sh" || true
    pkill -f 'org.apache.spark.deploy.master.Master' || true
}

show_summary() {
    echo "-----------------------------------------------------------------"
    echo "Shutdown complete"
    echo "Engine: $ENGINE"
    echo "-----------------------------------------------------------------"
}

main() {
    prompt_inputs

    case "$ENGINE" in
        flink)
            shutdown_flink
            ;;
        spark)
            shutdown_spark
            ;;
        both)
            shutdown_flink
            shutdown_spark
            ;;
    esac

    show_summary
}

main "$@"
