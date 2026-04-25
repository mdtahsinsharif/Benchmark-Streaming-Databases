#!/bin/bash
set -euo pipefail

REL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARS_DIR="$REL_DIR/tars"
SCRIPTS_DIR="$REL_DIR/bin"
SSH_HELPER="$SCRIPTS_DIR/configure_workers.sh"
BENCHMARK_ENV_FILE="$SCRIPTS_DIR/.benchmark_env.sh"

SHELL_RC="$HOME/.bashrc"
[ -f "$HOME/.zshrc" ] && SHELL_RC="$HOME/.zshrc"

declare -a ENV_EXPORTS=()
declare -a PATH_ENTRIES=()

add_export() {
    local key="$1"
    local val="$2"
    ENV_EXPORTS+=("export ${key}=${val}")
}

add_path() {
    local val="$1"
    PATH_ENTRIES+=("$val")
}

persist_env() {
    mkdir -p "$(dirname "$BENCHMARK_ENV_FILE")"

    cat > "$BENCHMARK_ENV_FILE" <<'EOL'
#!/bin/bash
EOL

    local line
    for line in "${ENV_EXPORTS[@]}"; do
        echo "$line" >> "$BENCHMARK_ENV_FILE"
        if ! grep -qxF "$line" "$SHELL_RC"; then
            echo "$line" >> "$SHELL_RC"
        fi
    done

    if [ "${#PATH_ENTRIES[@]}" -gt 0 ]; then
        local joined=""
        local p
        for p in "${PATH_ENTRIES[@]}"; do
            joined+="${p}:"
        done
        local path_line="export PATH=${joined}\$PATH"
        echo "$path_line" >> "$BENCHMARK_ENV_FILE"
        if ! grep -qxF "$path_line" "$SHELL_RC"; then
            echo "$path_line" >> "$SHELL_RC"
        fi
    fi

    chmod +x "$BENCHMARK_ENV_FILE"
}

install_java() {
    if command -v java >/dev/null 2>&1; then
        echo "Java already installed."
    else
        echo "Installing OpenJDK 11..."
        sudo apt update
        sudo apt install -y openjdk-11-jdk
    fi

    local java_home
    java_home="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
    export JAVA_HOME="$java_home"
    add_export "JAVA_HOME" "$JAVA_HOME"
    add_path "\$JAVA_HOME/bin"

    echo "JAVA_HOME set to: $JAVA_HOME"
}

extract_archive() {
    local tar_path="$1"
    local expanded_name="$2"
    local target_dir="$3"

    if [ ! -f "$tar_path" ]; then
        echo "Missing archive: $tar_path"
        exit 1
    fi

    if [ -d "$target_dir" ]; then
        echo "Found existing $(basename "$target_dir"); skipping extraction."
        return
    fi

    echo "Extracting $(basename "$tar_path")..."
    (
        cd "$REL_DIR"
        tar -xzvf "$tar_path"
    )
    if [ ! -d "$REL_DIR/$expanded_name" ]; then
        echo "Expected extracted directory not found: $REL_DIR/$expanded_name"
        exit 1
    fi

    mv "$REL_DIR/$expanded_name" "$target_dir"
    echo "Prepared $target_dir"
}

ensure_kafka_client_jar() {
    local client_jar="$TARS_DIR/kafka-clients-3.7.1.jar"
    local target_dir="$REL_DIR/kafka/libs"

    if [ ! -d "$target_dir" ]; then
        echo "Kafka libs directory not found: $target_dir"
        exit 1
    fi

    if [ -f "$target_dir/kafka-clients-3.7.1.jar" ]; then
        return
    fi

    if [ ! -f "$client_jar" ]; then
        echo "Missing Kafka client jar: $client_jar"
        exit 1
    fi

    cp -f "$client_jar" "$target_dir/"
    echo "Copied kafka-clients-3.7.1.jar into $target_dir"
}

prepare_common_libs() {
    local mode="$1"

    mkdir -p "$SCRIPTS_DIR"

    # Kafka is common for both stacks in this benchmark setup.
    extract_archive "$TARS_DIR/kafka_2.12-3.7.1.tgz" "kafka_2.12-3.7.1" "$REL_DIR/kafka"
    ensure_kafka_client_jar

    case "$mode" in
        flink)
            extract_archive "$TARS_DIR/flink-1.18.0-bin-scala_2.12.tgz" "flink-1.18.0" "$REL_DIR/flink"
            ;;
        spark)
            extract_archive "$TARS_DIR/spark-3.5.8-bin-hadoop3.tgz" "spark-3.5.8-bin-hadoop3" "$REL_DIR/spark"
            ;;
        both)
            extract_archive "$TARS_DIR/flink-1.18.0-bin-scala_2.12.tgz" "flink-1.18.0" "$REL_DIR/flink"
            extract_archive "$TARS_DIR/spark-3.5.8-bin-hadoop3.tgz" "spark-3.5.8-bin-hadoop3" "$REL_DIR/spark"
            ;;
        *)
            echo "Unsupported mode: $mode"
            exit 1
            ;;
    esac
}

register_mode_envs() {
    local mode="$1"

    export KAFKA_HOME="$REL_DIR/kafka"
    add_export "KAFKA_HOME" "$KAFKA_HOME"
    add_path "\$KAFKA_HOME/bin"

    case "$mode" in
        flink)
            export FLINK_HOME="$REL_DIR/flink"
            export FLINK_CONF_DIR="$FLINK_HOME/conf"
            add_export "FLINK_HOME" "$FLINK_HOME"
            add_export "FLINK_CONF_DIR" "$FLINK_CONF_DIR"
            add_path "\$FLINK_HOME/bin"
            ;;
        spark)
            export SPARK_HOME="$REL_DIR/spark"
            export SPARK_CONF_DIR="$SPARK_HOME/conf"
            add_export "SPARK_HOME" "$SPARK_HOME"
            add_export "SPARK_CONF_DIR" "$SPARK_CONF_DIR"
            add_path "\$SPARK_HOME/bin"
            add_path "\$SPARK_HOME/sbin"
            ;;
        both)
            export FLINK_HOME="$REL_DIR/flink"
            export FLINK_CONF_DIR="$FLINK_HOME/conf"
            export SPARK_HOME="$REL_DIR/spark"
            export SPARK_CONF_DIR="$SPARK_HOME/conf"
            add_export "FLINK_HOME" "$FLINK_HOME"
            add_export "FLINK_CONF_DIR" "$FLINK_CONF_DIR"
            add_export "SPARK_HOME" "$SPARK_HOME"
            add_export "SPARK_CONF_DIR" "$SPARK_CONF_DIR"
            add_path "\$FLINK_HOME/bin"
            add_path "\$SPARK_HOME/bin"
            add_path "\$SPARK_HOME/sbin"
            ;;
    esac
}

configure_flink() {
    echo "---------------------------------------------"
    echo "Configuring Flink..."

    export FLINK_HOME="$REL_DIR/flink"
    export FLINK_CONF_DIR="$FLINK_HOME/conf"
    export KAFKA_HOME="$REL_DIR/kafka"

    add_export "FLINK_HOME" "$FLINK_HOME"
    add_export "FLINK_CONF_DIR" "$FLINK_CONF_DIR"
    add_export "KAFKA_HOME" "$KAFKA_HOME"
    add_path "\$FLINK_HOME/bin"
    add_path "\$KAFKA_HOME/bin"

    mkdir -p "$FLINK_CONF_DIR" "$FLINK_HOME/log"

    local connector_src="$TARS_DIR/flink-sql-connector-kafka-3.0.1-1.18.jar"
    if [ ! -f "$connector_src" ]; then
        echo "Missing Flink Kafka connector: $connector_src"
        exit 1
    fi
    cp -f "$connector_src" "$FLINK_HOME/lib/"

    read -rp "JobManager IP [localhost]: " JM_IP
    JM_IP="${JM_IP:-localhost}"

    read -rp "JobManager RPC port [6123]: " JM_PORT
    JM_PORT="${JM_PORT:-6123}"

    read -rp "JobManager memory [1024m]: " JM_MEM
    JM_MEM="${JM_MEM:-1024m}"

    read -rp "TaskManager slots [4]: " TM_SLOTS
    TM_SLOTS="${TM_SLOTS:-4}"

    read -rp "Default parallelism [4]: " PARALLELISM
    PARALLELISM="${PARALLELISM:-4}"

    read -rp "TaskManager memory [2048m]: " TM_MEM
    TM_MEM="${TM_MEM:-2048m}"

    read -rp "TaskManager temp dirs [/tmp/flink]: " TMP_DIRS
    TMP_DIRS="${TMP_DIRS:-/tmp/flink}"

    cat > "$FLINK_CONF_DIR/flink-conf.yaml" <<EOL
#==============================================================================
# MANAGER CONFIG
#==============================================================================
# JobManager settings
#==============================================================================

jobmanager.rpc.address: $JM_IP
jobmanager.rpc.port: $JM_PORT
jobmanager.bind-host: 0.0.0.0
jobmanager.memory.process.size: $JM_MEM

rest.address: $JM_IP
rest.bind-address: 0.0.0.0
rest.port: 8081

jobmanager.execution.failover-strategy: region
parallelism.default: $PARALLELISM

#==============================================================================
# Advanced settings
#==============================================================================

io.tmp.dirs: /tmp/flink/io

#==============================================================================
# JAVA options
#==============================================================================

env.java.opts: -verbose:gc -XX:NewRatio=3 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4
env.java.opts.jobmanager: -Xloggc:\$FLINK_LOG_DIR/jobmanager-gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=512M
env.java.opts.taskmanager: -Xloggc:\$FLINK_LOG_DIR/taskmanager-gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=512M

#==============================================================================
# State & Checkpointing
#==============================================================================

state.backend: rocksdb
state.checkpoints.dir: file:///tmp/flink/checkpoints
state.backend.rocksdb.localdir: /tmp/flink/rocksdb
state.backend.incremental: true
execution.checkpointing.interval: 180000
execution.checkpointing.mode: EXACTLY_ONCE
state.backend.local-recovery: true

#==============================================================================
# Runtime Others
#==============================================================================

table.exec.mini-batch.enabled: true
table.exec.mini-batch.allow-latency: 2s
table.exec.mini-batch.size: 50000
table.optimizer.distinct-agg.split.enabled: true
execution.checkpointing.checkpoints-after-tasks-finish.enabled: false

EOL

    cat > "$FLINK_CONF_DIR/flink-conf-w.yaml" <<EOL
#==============================================================================
# WORKER CONFIG
#==============================================================================
# JobManager settings
#==============================================================================

jobmanager.rpc.address: $JM_IP
jobmanager.rpc.port: $JM_PORT
jobmanager.bind-host: 0.0.0.0
jobmanager.memory.process.size: $JM_MEM

rest.address: $JM_IP
rest.bind-address: 0.0.0.0
rest.port: 8081

jobmanager.execution.failover-strategy: region

#==============================================================================
# TaskManager settings
#==============================================================================

taskmanager.host: <IP_ADDRESS>
taskmanager.memory.process.size: $TM_MEM
taskmanager.numberOfTaskSlots: $TM_SLOTS

#==============================================================================
# Advanced settings
#==============================================================================

io.tmp.dirs: /tmp/flink/io

#==============================================================================
# JAVA options
#==============================================================================

env.java.opts: -verbose:gc -XX:NewRatio=3 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4
env.java.opts.jobmanager: -Xloggc:\$FLINK_LOG_DIR/jobmanager-gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=512M
env.java.opts.taskmanager: -Xloggc:\$FLINK_LOG_DIR/taskmanager-gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=512M

#==============================================================================
# State & Checkpointing
#==============================================================================

state.backend: rocksdb
state.checkpoints.dir: file:///tmp/flink/checkpoints
state.backend.rocksdb.localdir: /tmp/flink/rocksdb
state.backend.incremental: true
execution.checkpointing.interval: 180000
execution.checkpointing.mode: EXACTLY_ONCE
state.backend.local-recovery: true

#==============================================================================
# Runtime Others
#==============================================================================

table.exec.mini-batch.enabled: true
table.exec.mini-batch.allow-latency: 2s
table.exec.mini-batch.size: 50000
table.optimizer.distinct-agg.split.enabled: true
execution.checkpointing.checkpoints-after-tasks-finish.enabled: false
EOL

    echo "$JM_IP:$JM_PORT" > "$FLINK_CONF_DIR/masters"

    read -rp "Number of TaskManagers [1]: " NUM_TM
    NUM_TM="${NUM_TM:-1}"

    : > "$FLINK_CONF_DIR/workers"
    local i tm_ip
    for ((i=1; i<=NUM_TM; i++)); do
        read -rp "TaskManager #$i IP [localhost]: " tm_ip
        tm_ip="${tm_ip:-localhost}"
        echo "$tm_ip" >> "$FLINK_CONF_DIR/workers"
    done

    add_export "JM_IP" "$JM_IP"
    add_export "JM_PORT" "$JM_PORT"
}

configure_spark() {
    echo "---------------------------------------------"
    echo "Configuring Spark..."

    export SPARK_HOME="$REL_DIR/spark"
    export SPARK_CONF_DIR="$SPARK_HOME/conf"
    export KAFKA_HOME="$REL_DIR/kafka"

    add_export "SPARK_HOME" "$SPARK_HOME"
    add_export "SPARK_CONF_DIR" "$SPARK_CONF_DIR"
    add_export "KAFKA_HOME" "$KAFKA_HOME"
    add_path "\$SPARK_HOME/bin"
    add_path "\$SPARK_HOME/sbin"
    add_path "\$KAFKA_HOME/bin"

    mkdir -p "$SPARK_CONF_DIR" "$SPARK_HOME/logs" "$SPARK_HOME/work"

    local spark_connector_src="$TARS_DIR/spark-sql-kafka-0-10_2.12-3.5.0.jar"
    if [ ! -f "$spark_connector_src" ]; then
        echo "Missing Spark Kafka connector: $spark_connector_src"
        exit 1
    fi
    cp -f "$spark_connector_src" "$SPARK_HOME/jars/"

    if [ -f "$SPARK_HOME/conf/spark-env.sh.template" ] && [ ! -f "$SPARK_CONF_DIR/spark-env.sh" ]; then
        cp "$SPARK_HOME/conf/spark-env.sh.template" "$SPARK_CONF_DIR/spark-env.sh"
    fi

    read -rp "Master IP [localhost]: " MASTER_IP
    MASTER_IP="${MASTER_IP:-localhost}"

    read -rp "Master port [7077]: " MASTER_PORT
    MASTER_PORT="${MASTER_PORT:-7077}"

    read -rp "Master Web UI port [8080]: " WEBUI_PORT
    WEBUI_PORT="${WEBUI_PORT:-8080}"

    read -rp "Number of workers in cluster [1]: " NUM_WORKERS
    NUM_WORKERS="${NUM_WORKERS:-1}"

    read -rp "Worker cores per node [4]: " WORKER_CORES
    WORKER_CORES="${WORKER_CORES:-4}"

    read -rp "Worker memory per node [2g]: " WORKER_MEM
    WORKER_MEM="${WORKER_MEM:-2g}"

    read -rp "Worker instances per node [1]: " WORKER_INSTANCES
    WORKER_INSTANCES="${WORKER_INSTANCES:-1}"

    local total_parallelism=$((WORKER_CORES * WORKER_INSTANCES * NUM_WORKERS))
    local shuffle_partitions=$((total_parallelism * 2))

    cat > "$SPARK_CONF_DIR/spark-defaults.conf" <<EOL
spark.master                     spark://$MASTER_IP:$MASTER_PORT
spark.eventLog.enabled           true
spark.eventLog.dir               file://$SPARK_HOME/logs
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.driver.memory              1g
spark.executor.memory            ${WORKER_MEM}
spark.executor.cores             ${WORKER_CORES}
spark.default.parallelism        ${total_parallelism}
spark.sql.shuffle.partitions     ${shuffle_partitions}
spark.network.timeout            120s
spark.executor.heartbeatInterval 20s
spark.rpc.askTimeout             60s
EOL

    cat > "$SPARK_CONF_DIR/spark-env.sh" <<EOL
#!/bin/bash
export JAVA_HOME=$JAVA_HOME
export SPARK_MASTER_HOST=$MASTER_IP
export SPARK_MASTER_PORT=$MASTER_PORT
export SPARK_MASTER_WEBUI_PORT=$WEBUI_PORT
export SPARK_WORKER_CORES=$WORKER_CORES
export SPARK_WORKER_MEMORY=$WORKER_MEM
export SPARK_WORKER_INSTANCES=$WORKER_INSTANCES
export SPARK_LOG_DIR=$SPARK_HOME/logs
export SPARK_WORKER_DIR=$SPARK_HOME/work
EOL

    cat > "$SPARK_CONF_DIR/spark-env-worker.sh.template" <<EOL
#!/bin/bash
export JAVA_HOME=$JAVA_HOME
export SPARK_MASTER_HOST=$MASTER_IP
export SPARK_MASTER_PORT=$MASTER_PORT
export SPARK_MASTER_WEBUI_PORT=$WEBUI_PORT
export SPARK_WORKER_CORES=$WORKER_CORES
export SPARK_WORKER_MEMORY=$WORKER_MEM
export SPARK_WORKER_INSTANCES=$WORKER_INSTANCES
export SPARK_LOG_DIR=$SPARK_HOME/logs
export SPARK_WORKER_DIR=$SPARK_HOME/work
export SPARK_LOCAL_IP=<IP_ADDRESS>
export SPARK_WORKER_HOST=<IP_ADDRESS>
EOL

    chmod +x "$SPARK_CONF_DIR/spark-env.sh"
    chmod +x "$SPARK_CONF_DIR/spark-env-worker.sh.template"

    : > "$SPARK_CONF_DIR/workers"
    local i w_ip
    for ((i=1; i<=NUM_WORKERS; i++)); do
        read -rp "Worker #$i IP [localhost]: " w_ip
        w_ip="${w_ip:-localhost}"
        echo "$w_ip" >> "$SPARK_CONF_DIR/workers"
    done

    add_export "SPARK_MASTER_HOST" "$MASTER_IP"
    add_export "SPARK_MASTER_PORT" "$MASTER_PORT"
    add_export "SPARK_MASTER_WEBUI_PORT" "$WEBUI_PORT"

    echo "Spark configured at $SPARK_CONF_DIR"
    echo "Spark worker template written to $SPARK_CONF_DIR/spark-env-worker.sh.template"
}

main() {
    echo "============================================="
    echo "Web-Scale Unified Setup"
    echo "This script can run on a master or worker node."
    echo "============================================="

    echo "Choose setup target:"
    echo "  1) Flink"
    echo "  2) Spark"
    echo "  3) Both"
    read -rp "Enter choice [1/2/3]: " CHOICE

    local mode
    case "$CHOICE" in
        1) mode="flink" ;;
        2) mode="spark" ;;
        3) mode="both" ;;
        *)
            echo "Invalid choice. Use 1, 2, or 3."
            exit 1
            ;;
    esac

    read -rp "Is this node Master or Worker? (m/w) [m]: " NODE_ROLE
    NODE_ROLE="${NODE_ROLE:-m}"

    install_java
    prepare_common_libs "$mode"
    register_mode_envs "$mode"

    if [ "$NODE_ROLE" = "w" ]; then
        persist_env
        echo "---------------------------------------------"
        echo "Worker node setup complete for: $mode"
        echo "Archives were extracted and environment variables were persisted."
        echo "No cluster configuration was generated on this node."
        echo "Run source $BENCHMARK_ENV_FILE or source $SHELL_RC to load environment variables."
        return
    fi

    case "$mode" in
        flink)
            configure_flink
            ;;
        spark)
            configure_spark
            ;;
        both)
            configure_flink
            configure_spark
            ;;
    esac

    echo "---------------------------------------------"
    echo "Setup complete for: $mode"
    if [ -x "$SSH_HELPER" ]; then
        read -rp "Configure remote workers now via SSH? (y/n) [y]: " CONFIGURE_REMOTE
        CONFIGURE_REMOTE="${CONFIGURE_REMOTE:-y}"

        if [ "$CONFIGURE_REMOTE" = "y" ]; then
            read -rp "SSH user for worker nodes: " SSH_USER
            if [ -n "${SSH_USER:-}" ]; then
                "$SSH_HELPER" "$mode" "$SSH_USER"
            else
                echo "Skipping remote worker configuration because no SSH user was provided."
            fi
        fi
    else
        echo "Worker helper not found or not executable: $SSH_HELPER"
    fi

    persist_env

    echo "Environment variables saved to $BENCHMARK_ENV_FILE"
    echo "source $BENCHMARK_ENV_FILE or source $SHELL_RC to load environment variables in current shell."
}

main "$@"
