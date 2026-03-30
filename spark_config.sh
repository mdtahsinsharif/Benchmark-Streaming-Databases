#!/bin/bash
set -e

# -----------------------------
# SYSTEM SETUP
# -----------------------------
echo "Installing OpenJDK 11..."
sudo apt update
sudo apt install -y openjdk-11-jdk

# -----------------------------
# JAVA SETUP
# -----------------------------
JAVA_HOME_PATH=$(dirname $(dirname $(readlink -f $(which java))))
export JAVA_HOME="$JAVA_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH"
echo "JAVA_HOME set to: $JAVA_HOME"

SHELL_RC="$HOME/.bashrc"
[ -f "$HOME/.zshrc" ] && SHELL_RC="$HOME/.zshrc"

# -----------------------------
# PATHS
# -----------------------------
REL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SPARK_TAR="$REL_DIR/tars/spark-3.5.8-bin-hadoop3.tgz"
KAFKA_TAR="$REL_DIR/tars/kafka_2.12-3.7.1.tgz"

SPARK_DIR="$REL_DIR/spark"
KAFKA_DIR="$REL_DIR/kafka"

# -----------------------------
# INSTALL KAFKA (IF NOT EXISTS)
# -----------------------------
if [ -d "$KAFKA_DIR" ]; then
    echo "Kafka directory exists, skipping extraction"
else
    echo "Installing Kafka..."
    [ ! -f "$KAFKA_TAR" ] && echo "Kafka tarball not found" && exit 1

    tar -xvzf "$KAFKA_TAR" -C "$REL_DIR"
    mv "$REL_DIR/kafka_2.12-3.7.1" "$KAFKA_DIR"
fi

# -----------------------------
# INSTALL SPARK
# -----------------------------
[ ! -f "$SPARK_TAR" ] && echo "Spark tarball not found" && exit 1

if [ -d "$SPARK_DIR" ]; then
    echo "Spark exists, skipping extraction"
else
    tar -xvzf "$SPARK_TAR" -C "$REL_DIR"
    mv "$REL_DIR/spark-3.5.8-bin-hadoop3" "$SPARK_DIR"
fi

# -----------------------------
# EXPORT ENV
# -----------------------------
export SPARK_HOME="$SPARK_DIR"
export KAFKA_HOME="$KAFKA_DIR"

export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$KAFKA_HOME/bin:$PATH"

# -----------------------------
# PERSIST ENV VARIABLES
# -----------------------------
for var in JAVA_HOME SPARK_HOME KAFKA_HOME; do
    grep -qxF "export $var=${!var}" "$SHELL_RC" || \
    echo "export $var=${!var}" >> "$SHELL_RC"
done

grep -qxF "export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$KAFKA_HOME/bin:\$PATH" "$SHELL_RC" || \
echo "export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$KAFKA_HOME/bin:\$PATH" >> "$SHELL_RC"

# -----------------------------
# SPARK CONFIG
# -----------------------------
SPARK_CONF_DIR="$SPARK_HOME/conf"
mkdir -p "$SPARK_CONF_DIR"

cp "$SPARK_HOME/conf/spark-env.sh.template" "$SPARK_CONF_DIR/spark-env.sh" 2>/dev/null || true

echo "-----------------------------------------------------------------"
echo "Configuring Spark cluster..."

# Role selection
read -p "Is this node MASTER or WORKER? (m/w): " NODE_ROLE

read -p "Enter Master IP (default: localhost): " MASTER_IP
MASTER_IP=${MASTER_IP:-localhost}

read -p "Enter Master port (default: 7077): " MASTER_PORT
MASTER_PORT=${MASTER_PORT:-7077}

read -p "Enter Web UI port (default: 8080): " WEBUI_PORT
WEBUI_PORT=${WEBUI_PORT:-8080}

# Worker config
if [[ "$NODE_ROLE" == "w" ]]; then
    read -p "Worker cores (default: 4): " WORKER_CORES
    WORKER_CORES=${WORKER_CORES:-4}

    read -p "Worker memory (default: 2g): " WORKER_MEM
    WORKER_MEM=${WORKER_MEM:-2g}

    read -p "Worker instances (default: 1): " WORKER_INSTANCES
    WORKER_INSTANCES=${WORKER_INSTANCES:-1}
fi

# -----------------------------
# EXPORT SPARK ENV VARIABLES
# -----------------------------

export JAVA_HOME="$JAVA_HOME"

export SPARK_MASTER_HOST="$MASTER_IP"
export SPARK_MASTER_PORT="$MASTER_PORT"
export SPARK_MASTER_WEBUI_PORT="$WEBUI_PORT"

export SPARK_WORKER_CORES="${WORKER_CORES:-4}"
export SPARK_WORKER_MEMORY="${WORKER_MEM:-2g}"
export SPARK_WORKER_INSTANCES="${WORKER_INSTANCES:-1}"

export SPARK_LOG_DIR="$SPARK_HOME/logs"
export SPARK_WORKER_DIR="$SPARK_HOME/work"

# Make sure logs directory exists
mkdir -p "$SPARK_LOG_DIR"

# -----------------------------
# spark-defaults.conf
# -----------------------------
cat > "$SPARK_CONF_DIR/spark-defaults.conf" <<EOL
spark.master                     spark://$MASTER_IP:$MASTER_PORT

# Logging
spark.eventLog.enabled           true
spark.eventLog.dir               file://$SPARK_HOME/logs

# Serialization
spark.serializer                 org.apache.spark.serializer.KryoSerializer

# Resources (aligned with workers)
spark.driver.memory              1g
spark.executor.memory            ${WORKER_MEM:-2g}
spark.executor.cores             ${WORKER_CORES:-2}

# Parallelism (like Flink)
spark.default.parallelism        $((WORKER_CORES * NUM_WORKERS))
spark.sql.shuffle.partitions     $((WORKER_CORES * NUM_WORKERS * 2))

# Stability
spark.network.timeout            120s
spark.executor.heartbeatInterval 20s
spark.rpc.askTimeout             60s
EOL

# -----------------------------
# PERSIST SPARK ENV VARIABLES TO SHELL RC
# -----------------------------
SPARK_ENV_VARS=(
    SPARK_MASTER_HOST
    SPARK_MASTER_PORT
    SPARK_MASTER_WEBUI_PORT
)

for var in "${SPARK_ENV_VARS[@]}"; do
    # Avoid duplicate entries
    if ! grep -qxF "export $var=${!var}" "$SHELL_RC"; then
        echo "export $var=${!var}" >> "$SHELL_RC"
    fi
done

# -----------------------------
# WORKERS FILE
# -----------------------------
WORKERS_FILE="$SPARK_CONF_DIR/workers"
> "$WORKERS_FILE"

read -p "Enter number of workers: " NUM_WORKERS
NUM_WORKERS=${NUM_WORKERS:-1}

for ((i=1;i<=NUM_WORKERS;i++)); do
    read -p "Worker #$i IP (default: localhost): " W
    W=${W:-localhost}
    echo "$W" >> "$WORKERS_FILE"
done

# -----------------------------
# FINAL OUTPUT
# -----------------------------

echo "-----------------------------------------------------------------"
echo "✅ Spark configuration written."
echo "  Role: $NODE_ROLE"
echo "  Master: $MASTER_IP:$MASTER_PORT"

if [[ "$NODE_ROLE" == "w" ]]; then
    echo "  Worker cores: $WORKER_CORES"
    echo "  Worker memory: $WORKER_MEM"
    echo "  Worker instances: $WORKER_INSTANCES"
fi

echo "-----------------------------------------------------------------"
echo "✅ Configuration complete."
echo "Run: source $SHELL_RC"