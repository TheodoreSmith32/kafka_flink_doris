#!/usr/bin/env bash
# Submit job PyFlink ke cluster.
#   ./flink/submit.sh 01_error_context
#   ./flink/submit.sh all        # semua use case (butuh slot cukup, TaskManager hanya 2 slot)
set -euo pipefail

JOBS=(01_error_context 02_transaction_journey 03_page_views_window \
      04_txn_summary_window 05_alert_error_spike 06_alert_failed_txn_streak)

submit() {
  echo ">> submit $1"
  docker exec flink-jobmanager flink run -d \
    -pyclientexec python -pyexec python \
    -py /opt/flink/jobs/run_sql.py -pyfs /opt/flink/jobs/common.py "$1"
}

[ $# -eq 1 ] || { echo "Pakai: $0 <use_case|all>"; printf '  %s\n' "${JOBS[@]}"; exit 1; }
if [ "$1" = all ]; then for j in "${JOBS[@]}"; do submit "$j"; done; else submit "$1"; fi
