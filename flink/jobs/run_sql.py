"""Entrypoint job PyFlink generik: jalankan satu use case dari flink/sql.

Contoh (dari host, lewat helper):
    ./flink/submit.sh 01_error_context
Atau manual:
    docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/run_sql.py 01_error_context
"""
import sys

from common import run_usecase

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Pakai: run_sql.py <nama_use_case>   mis. 01_error_context")
    run_usecase(sys.argv[1])
