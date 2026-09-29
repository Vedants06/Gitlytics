#!/usr/bin/env bash
# Install Airflow 2.11.2 and the project's Python packages into ~/gitlytics/.venv
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_VERSION=2.11.2
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

cd "$REPO"
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "$CONSTRAINTS"
pip install -r requirements.txt

airflow version
echo "Airflow ${AIRFLOW_VERSION} installed in $REPO/.venv"
