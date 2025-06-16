#!/usr/bin/env python3
import argparse
import logging
import subprocess
import os
import shutil
import sys
import duckdb
from datetime import datetime
import difflib


def setup_logger():
    logger = logging.getLogger("duckdb_merge")
    handler = logging.StreamHandler()
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def parse_args():
    p = argparse.ArgumentParser(
        description="Merge a remote DuckDB into a local one")
    p.add_argument("-H", "--host",       required=True, help="remote host")
    p.add_argument("-r", "--remote-file", required=True,
                   help="path to remote duckdb file")
    p.add_argument("-f", "--duckdb-file", required=True,
                   help="path to local duckdb file")
    p.add_argument("-U", "--user", required=True,
                   help="SSH username for remote operations")
    return p.parse_args()


def remote_wal_exists(user, host, wal_path):
    """Return True if a WAL file exists on the remote host for the given DuckDB path."""
    target = f"{user}@{host}"
    return subprocess.run(["ssh", target, "test", "-e", wal_path], check=False).returncode == 0


def scp_remote_file(user, host, remote_path, logger):
    base, _ = os.path.splitext(remote_path)
    wal_path = f"{base}.wal"
    if remote_wal_exists(user, host, wal_path):
        logger.error(
            "Detected unclosed WAL file on remote (%s), aborting.", wal_path)
        sys.exit(1)

    # determine a non-colliding local filename
    original = os.path.basename(remote_path)
    name, ext = os.path.splitext(original)
    local_name = original
    counter = 1
    while os.path.exists(local_name):
        logger.info("Local file %s exists, trying %s",
                    local_name, f"{name}-{counter}{ext}")
        local_name = f"{name}-{counter}{ext}"
        counter += 1

    cmd = ["scp", f"{user}@{host}:{remote_path}", local_name]
    logger.info("Copying remote file with: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, capture_output=True)
    return os.path.abspath(local_name)


def load_schema(conn):
    schema = {}
    for (tbl,) in conn.execute("SHOW TABLES").fetchall():
        cols = conn.execute(f"PRAGMA table_info('{tbl}')").fetchall()
        # each row: cid, name, type, notnull, dflt_value, pk
        schema[tbl] = [(c[1], c[2], bool(c[5])) for c in cols]
    return schema


def diff_schemas(s1, s2):
    lines1 = []
    lines2 = []
    for tbl in sorted(s1.keys() | s2.keys()):
        lines1.append(f"TABLE: {tbl}")
        for col in s1.get(tbl, []):
            lines1.append(f"  {col}")

        lines2.append(f"TABLE: {tbl}")
        for col in s2.get(tbl, []):
            lines2.append(f"  {col}")

    return "\n".join(difflib.unified_diff(lines1, lines2, fromfile="local", tofile="remote", lineterm=""))


def print_basic_info(conn, logger):
    logger.info("Local database table counts:")
    for (tbl,) in conn.execute("SHOW TABLES").fetchall():
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        logger.info("  %s: %d rows", tbl, cnt)


def backup_file(local_path, logger):
    base = os.path.splitext(os.path.basename(local_path))[0]
    now = datetime.now().strftime("%Y%m%d%H%M%S")

    backup = f"backup-{base}-{now}.duckdb"
    shutil.copy2(local_path, backup)

    logger.info("Backup created: %s", backup)
    return backup


def merge_databases(local_db, remote_db, logger):
    conn = duckdb.connect(local_db)
    try:
        conn.execute(f"ATTACH '{remote_db}' AS rem")
        conn.execute("BEGIN")

        for (tbl,) in conn.execute("SHOW TABLES").fetchall():
            # find PK columns
            info = conn.execute(f"PRAGMA table_info('{tbl}')").fetchall()
            pks = [row[1] for row in info if row[5] > 0]
            if pks:
                on_clause = " AND ".join(f"l.{c}=r.{c}" for c in pks)
                dup = conn.execute(
                    f"SELECT COUNT(*) FROM rem.{tbl} r JOIN main.{tbl} l ON {on_clause}"
                ).fetchone()[0]

                if dup:
                    raise RuntimeError(
                        f"Primary key conflict on table '{tbl}': {dup} duplicates found")

            logger.info("Inserting %s...", tbl)
            conn.execute(f"INSERT INTO {tbl} SELECT * FROM rem.{tbl}")

        conn.execute("COMMIT")

        logger.info("Merge complete.")
    except Exception:
        conn.execute("ROLLBACK")
        logger.error("Merge failed; rolled back any changes.")
        raise
    finally:
        conn.execute("DETACH rem")
        conn.close()


def check_date_overlap(conn_local, conn_remote, schema_local, logger):
    """Abort if any table's fetch_date or created_at columns have overlapping data."""

    for tbl, cols in schema_local.items():
        date_cols = [c[0]
                     for c in cols if c[0] in ('fetch_date', 'created_at')]

        for col in date_cols:
            local_max = conn_local.execute(
                f"SELECT MAX({col}) FROM {tbl}").fetchone()[0]
            remote_min = conn_remote.execute(
                f"SELECT MIN({col}) FROM {tbl}").fetchone()[0]

            if local_max is not None and remote_min is not None and remote_min <= local_max:
                overlap = conn_remote.execute(
                    f"SELECT COUNT(*) FROM {tbl} WHERE {col} <= {repr(local_max)}"
                ).fetchone()[0]
                logger.error(
                    "Error: Overlap detected in table '%s' on column '%s': %d overlapping rows (<= %s)",
                    tbl, col, overlap, local_max)

                sys.exit(1)


def main():
    args = parse_args()
    logger = setup_logger()

    try:
        remote_copy = scp_remote_file(
            args.user, args.host, args.remote_file, logger)

        with duckdb.connect(args.duckdb_file) as conn_local, \
                duckdb.connect(remote_copy) as conn_remote:
            schema_local = load_schema(conn_local)
            schema_remote = load_schema(conn_remote)

            diff = diff_schemas(schema_local, schema_remote)
            if diff:
                logger.error("Schema mismatch!\n%s", diff)
                sys.exit(1)

            print_basic_info(conn_local, logger)

            check_date_overlap(conn_local, conn_remote, schema_local, logger)

        backup_file(args.duckdb_file, logger)
        merge_databases(args.duckdb_file, remote_copy, logger)

        # delete remote DB only if no WAL present
        base, _ = os.path.splitext(args.remote_file)
        wal_path = f"{base}.wal"

        if remote_wal_exists(args.user, args.host, wal_path):
            logger.error(
                "Remote WAL file still present at %s; not deleting remote DB.", wal_path)
        else:
            logger.info("Deleting remote database file: %s", args.remote_file)
            subprocess.run(
                ["ssh", f"{args.user}@{args.host}", "rm", args.remote_file], check=True)

    except subprocess.CalledProcessError as e:
        logger.error("an ssh command failed: %s", e)
        logger.error(
            f"stderr = {e.stderr.decode('ascii').strip()}")
        logger.error(
            f"stdout = {e.stdout.decode('ascii')}"
        )
        sys.exit(1)

    except Exception as e:
        logger.error("Error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
