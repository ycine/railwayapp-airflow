#!/usr/bin/env bash
set -euo pipefail

export AIRFLOW__CORE__EXECUTOR="${AIRFLOW__CORE__EXECUTOR:-SequentialExecutor}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-sqlite:////opt/airflow/data/airflow.db}"
export AIRFLOW__CORE__LOAD_EXAMPLES="${AIRFLOW__CORE__LOAD_EXAMPLES:-False}"
export AIRFLOW__API__HOST="${AIRFLOW__API__HOST:-0.0.0.0}"
export AIRFLOW__API__PORT="${AIRFLOW__API__PORT:-${PORT:-8080}}"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="${AIRFLOW__LOGGING__BASE_LOG_FOLDER:-/opt/airflow/data/logs}"

# ⭐ Zrób to NAJPIERW, zanim cokolwiek spróbuje pisać do /opt/airflow/data
mkdir -p /opt/airflow/data
chown -R airflow:root /opt/airflow/data
chmod -R 775 /opt/airflow/data

export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-/opt/airflow/data/simple_auth_manager_passwords.json.generated}"

if [[ -n "${_AIRFLOW_WWW_USER_USERNAME:-}" ]]; then
  export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="${_AIRFLOW_WWW_USER_USERNAME}:Admin"
fi

if [[ -n "${_AIRFLOW_WWW_USER_USERNAME:-}" && -n "${_AIRFLOW_WWW_USER_PASSWORD:-}" ]]; then
  python3 - "$AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE" <<'PYEOF'
import json
import os
import sys
from pathlib import Path
path = Path(sys.argv[1])
username = os.environ["_AIRFLOW_WWW_USER_USERNAME"]
password = os.environ["_AIRFLOW_WWW_USER_PASSWORD"]
data = {}
if path.exists():
    try:
        loaded = json.loads(path.read_text())
        if not isinstance(loaded, dict):
            raise ValueError("passwords file did not contain a JSON object")
        data = loaded
    except (json.JSONDecodeError, ValueError) as exc:
        backup = path.with_suffix(path.suffix + ".bak")
        path.rename(backup)
        print(
            f"railway-entrypoint: existing passwords file was not valid JSON "
            f"({exc}); moved it to {backup} and starting a fresh one",
            file=sys.stderr,
        )
data[username] = password
path.parent.mkdir(parents=True, exist_ok=True)
tmp_path = path.with_suffix(path.suffix + ".tmp")
tmp_path.write_text(json.dumps(data))
tmp_path.chmod(0o600)
tmp_path.replace(path)
PYEOF
fi

# NOTE: no alembic/DB hack here — a corrupted migration should be fixed via a
# proper `airflow db migrate`, never a hand-crafted DELETE against alembic_version.
exec /entrypoint airflow standalone