"""Helper bersama untuk job PyFlink yang menjalankan Flink SQL dari folder flink/sql.

Alurnya sama seperti `sql-client.sh -i 00_init.sql -f 0X_xxx.sql`:
  1. jalankan 00_init.sql (SET + CREATE TEMPORARY TABLE source/sink)
  2. jalankan file use case (berisi INSERT INTO ... SELECT ...)
"""
import re
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment

# Di container: ./flink/sql di-mount ke /opt/flink/sql
SQL_DIR = Path("/opt/flink/sql")

SET_RE = re.compile(r"^SET\s+'([^']+)'\s*=\s*'([^']*)'$", re.IGNORECASE | re.DOTALL)


def read_statements(path: Path) -> list:
    """Baca file SQL -> daftar statement (buang komentar `--`, pisah dengan `;`)."""
    text = "\n".join(line.split("--", 1)[0] for line in path.read_text().splitlines())
    return [s.strip() for s in text.split(";") if s.strip()]


def build_env() -> TableEnvironment:
    return TableEnvironment.create(EnvironmentSettings.in_streaming_mode())


def run_statements(t_env: TableEnvironment, path: Path) -> list:
    """Jalankan semua statement. SET ditangani lewat config, sisanya execute_sql.

    Return: daftar TableResult dari INSERT (job streaming yang terkirim).
    """
    results = []
    for stmt in read_statements(path):
        m = SET_RE.match(stmt)
        if m:
            t_env.get_config().set(m.group(1), m.group(2))
            continue
        res = t_env.execute_sql(stmt)
        if stmt.upper().startswith("INSERT"):
            results.append(res)
    return results


def run_usecase(name: str, pipeline_name: str = None) -> None:
    """name: nama file tanpa .sql, mis. '01_error_context'."""
    usecase = SQL_DIR / f"{name}.sql"
    if not usecase.exists():
        raise SystemExit(f"File SQL tidak ditemukan: {usecase}")

    t_env = build_env()
    run_statements(t_env, SQL_DIR / "00_init.sql")
    t_env.get_config().set("pipeline.name", pipeline_name or name)
    results = run_statements(t_env, usecase)

    for res in results:
        job = res.get_job_client()
        print(f"Job terkirim: {name} -> {job.get_job_id() if job else '(lokal)'}")
