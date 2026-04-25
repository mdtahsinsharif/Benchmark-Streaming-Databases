# Cluster Setup and Execution Guide

This document describes the required steps to initialize the environment and start the cluster.

---

## 1. Create Conda Environment

Ensure Conda is installed and available in your shell.

Create the environment from the provided environment file:

```bash
conda env create -f environment.yml

conda activate <env_name>

All worker nodes should have the project in the same folder directory to execute automatically from the manager node.

./bin/cluster_config.sh

./bin/configure_workers.sh

source ~/.bashrc

./bin/start.sh

To shutdown the cluster please use 
./bin/shutdown.sh