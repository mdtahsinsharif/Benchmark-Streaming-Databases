#!/bin/bash
set -e  # stop on error

# -----------------------------
# SYSTEM SETUP
# -----------------------------
echo "Installing OpenJDK 11..."
sudo apt update
sudo apt install -y openjdk-11-jdk

# -----------------------------
# JAVA SETUP
# -----------------------------

echo "-----------------------------------------------------------------"
JAVA_HOME_PATH=$(dirname $(dirname $(readlink -f $(which java))))
export JAVA_HOME="$JAVA_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH"
echo "JAVA_HOME set to: $JAVA_HOME"

# Detect shell config
SHELL_RC="$HOME/.bashrc"
[ -f "$HOME/.zshrc" ] && SHELL_RC="$HOME/.zshrc"

# -----------------------------
# PATHS (relative to script)
# -----------------------------
REL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_TAR="$REL_DIR/tars/kafka_2.12-3.7.1.tgz"
FLINK_TAR="$REL_DIR/tars/flink-1.18.0-bin-scala_2.12.tgz"
FLINK_CONNECTOR_JAR="$REL_DIR/tars/flink-sql-connector-kafka-3.0.1-1.18.jar"

KAFKA_DIR="$REL_DIR/kafka"
FLINK_DIR="$REL_DIR/flink"

# -----------------------------
# EXTRACT KAFKA AND FLINK
# -----------------------------
[ ! -f "$KAFKA_TAR" ] && echo "Kafka tarball not found" && exit 1
[ ! -f "$FLINK_TAR" ] && echo "Flink tarball not found" && exit 1

[ -d "$KAFKA_DIR" ] && echo "Kafka directory exists, skipping extraction" || \
(tar -xvzf "$KAFKA_TAR" -C "$REL_DIR" && mv "$REL_DIR/kafka_2.12-3.7.1" "$KAFKA_DIR")

[ -d "$FLINK_DIR" ] && echo "Flink directory exists, skipping extraction" || \
(tar -xzf "$FLINK_TAR" -C "$REL_DIR" && mv "$REL_DIR/flink-1.18.0" "$FLINK_DIR")

export KAFKA_HOME="$KAFKA_DIR"
export FLINK_HOME="$FLINK_DIR"
export FLINK_CONF_DIR="$FLINK_HOME/conf"
export PATH="$JAVA_HOME/bin:$KAFKA_HOME/bin:$FLINK_HOME/bin:$PATH"

# -----------------------------
# COPY FLINK KAFKA CONNECTOR
# -----------------------------
[ ! -f "$FLINK_CONNECTOR_JAR" ] && echo "Flink Kafka connector JAR not found" && exit 1
cp "$FLINK_CONNECTOR_JAR" "$FLINK_HOME/lib/"

# -----------------------------
# PERSIST ENV VARIABLES
# -----------------------------
for var in JAVA_HOME KAFKA_HOME FLINK_HOME FLINK_CONF_DIR; do
    grep -qxF "export $var=${!var}" "$SHELL_RC" || echo "export $var=${!var}" >> "$SHELL_RC"
done

for path_var in JAVA_HOME KAFKA_HOME FLINK_HOME; do
    grep -qxF "export PATH=\$${path_var}/bin:\$PATH" "$SHELL_RC" || \
    echo "export PATH=\$${path_var}/bin:\$PATH" >> "$SHELL_RC"
done

# -----------------------------
# FLINK CONFIGURATION
# -----------------------------
mkdir -p "$FLINK_CONF_DIR" "$FLINK_HOME/log"

echo "-----------------------------------------------------------------"
echo "Configuring Flink cluster..."

# Role selection
read -p "Is this node a JobManager or TaskManager? (jm/tm): " NODE_ROLE

# -----------------------------
# JOBMANAGER (COMMON CONFIG)
# -----------------------------
read -p "Enter JobManager IP (default: localhost): " JM_IP
JM_IP=${JM_IP:-localhost}

read -p "Enter JobManager RPC port (default: 6123): " JM_PORT
JM_PORT=${JM_PORT:-6123}

read -p "Enter JobManager memory (default: 1024m): " JM_MEM
JM_MEM=${JM_MEM:-1024m}

# -----------------------------
# TASKMANAGER CONFIG (if worker)
# -----------------------------
if [[ "$NODE_ROLE" == "tm" ]]; then
    read -p "Enter number of slots per TaskManager (default: 4): " TM_SLOTS
    TM_SLOTS=${TM_SLOTS:-4}

    read -p "Enter default job parallelism (default: 4): " PARALLELISM
    PARALLELISM=${PARALLELISM:-4}

    read -p "Enter TaskManager memory (default: 2048m): " TM_MEM
    TM_MEM=${TM_MEM:-2048m}

    read -p "Enter TaskManager temp dirs (comma-separated, default: /tmp/flink): " TMP_DIRS
    TMP_DIRS=${TMP_DIRS:-/tmp/flink}
fi

# -----------------------------
# WRITE flink-conf.yaml
# -----------------------------
cat > "$FLINK_CONF_DIR/flink-conf.yaml" <<EOL
jobmanager.rpc.address: $JM_IP
jobmanager.rpc.port: $JM_PORT

rest.address: $JM_IP
rest.bind-address: 0.0.0.0
rest.port: 8081

jobmanager.memory.process.size: $JM_MEM

env.java.opts: -XX:+UseG1GC -XX:MaxGCPauseMillis=200

web.log.path: $FLINK_HOME/log
EOL

# Append worker-specific config
if [[ "$NODE_ROLE" == "tm" ]]; then
cat >> "$FLINK_CONF_DIR/flink-conf.yaml" <<EOL
taskmanager.numberOfTaskSlots: $TM_SLOTS
parallelism.default: $PARALLELISM
taskmanager.memory.process.size: $TM_MEM
taskmanager.tmp.dirs: $TMP_DIRS

env.java.opts.taskmanager: -Xms$TM_MEM -Xmx$TM_MEM
EOL
fi

# -----------------------------
# CLUSTER TOPOLOGY CONFIG
# -----------------------------
echo "$JM_IP:$JM_PORT" > "$FLINK_CONF_DIR/masters"

read -p "Enter number of TaskManagers in cluster: " NUM_TM
NUM_TM=${NUM_TM:-1}

> "$FLINK_CONF_DIR/workers"
for ((i=1;i<=NUM_TM;i++)); do
    read -p "Enter TaskManager #$i IP (default: localhost): " TM
    TM=${TM:-localhost}
    echo "$TM" >> "$FLINK_CONF_DIR/workers"
done

# -----------------------------
# PERSIST FLINK ENV VARIABLES TO SHELL RC
# -----------------------------
FLINK_ENV_VARS=(
    JM_IP
    JM_PORT
)

for var in "${FLINK_ENV_VARS[@]}"; do
    # Skip empty variables (e.g., TM-specific vars on JobManager node)
    [ -z "${!var}" ] && continue

    # Append if not already present
    if ! grep -qxF "export $var=${!var}" "$SHELL_RC"; then
        echo "export $var=${!var}" >> "$SHELL_RC"
    fi
done

echo "✅ Flink IP/Port and TaskManager variables appended to $SHELL_RC"
echo "Run: source $SHELL_RC to load them permanently."

# -----------------------------
# FINAL OUTPUT
# -----------------------------

echo "-----------------------------------------------------------------"
echo "✅ Flink configuration written."
echo "  Role: $NODE_ROLE"
echo "  JobManager: $JM_IP:$JM_PORT"

if [[ "$NODE_ROLE" == "tm" ]]; then
    echo "  Slots: $TM_SLOTS"
    echo "  Parallelism: $PARALLELISM"
    echo "  TM Memory: $TM_MEM"
    echo "  TMP Dirs: $TMP_DIRS"
fi

echo "-----------------------------------------------------------------"
echo "✅ Configuration complete."
echo "Run: source $SHELL_RC"
