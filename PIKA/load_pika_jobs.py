#!/usr/bin/env python3
import argparse
import duckdb
import zipfile
import json
import logging
import os
from datetime import datetime, timedelta

# Configure logging


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def init_db(conn):
    """
    Create tables if they do not exist.
    """
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS jobs (
            id VARCHAR PRIMARY KEY,
            user VARCHAR,
            project VARCHAR,
            start_time TIMESTAMP,
            num_nodes INTEGER,
            num_hardware_threads INTEGER,
            name VARCHAR,
            script VARCHAR,
            duration_s INTEGER,
        )
        '''
    )

    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS stats (
            job_id VARCHAR,
            hostname VARCHAR,
            hardware_thread_id VARCHAR,
            date_time TIMESTAMP,
            unit VARCHAR,
            value DOUBLE,
            metric VARCHAR,
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )
        '''
    )


def insert_job(conn, meta):
    """
    Insert job record into the jobs table.
    """
    try:
        job_id = str(meta['jobId'])
        conn.execute(
            "INSERT INTO jobs (id, user, project, start_time, num_nodes, num_hardware_threads, name, script, duration_s)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                job_id,
                meta.get('user'),
                meta.get('project'),
                datetime.fromtimestamp(meta.get('startTime', 0)),
                meta.get('numNodes'),
                meta.get('numHwthreads'),
                meta.get('metaData', {}).get('jobName'),
                meta.get('metaData', {}).get('jobScript'),
                meta.get('duration')
            ]
        )
        logging.info(f"Inserted job {job_id}")
    except Exception as e:
        logging.error(f"Failed to insert job {meta.get('jobId')}: {e}")
        raise


def insert_stats(conn, job_id, start_ts, data_json):
    """
    Insert performance statistics for a job into the stats table.
    """
    metrics = ["cpu_used", "ipc", "flops_any",
               "mem_bw", "net_bw", "mem_used", "cpu_power"]
    inserted = 0

    for metric in metrics:
        metric_obj = data_json.get(metric)
        if not metric_obj:
            logging.warning(
                f"Metric {metric} missing in data.json for job {job_id}")
            continue
        # Expect one level: core/socket/node
        for level, lvl_data in metric_obj.items():
            if level == 'unit':
                continue

            try:
                unit = lvl_data['unit']['base']
                timestep = int(lvl_data['timestep'])
                series_list = lvl_data.get('series', [])

            except KeyError as e:
                logging.error(
                    f"Malformed {metric}.{level} in job {job_id}: missing {e}")
                continue

            for series in series_list:
                hostname = series.get('hostname')
                hw_id = series.get('id')
                values = series.get('data', [])
                for idx, val in enumerate(values):
                    dt = start_ts + timedelta(seconds=idx * timestep)
                    try:
                        conn.execute(
                            "INSERT INTO stats (job_id, hostname, hardware_thread_id, date_time, unit, value, metric)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?)",
                            [job_id, hostname, hw_id, dt, unit, val, metric]
                        )
                        inserted = inserted + 1
                    except Exception as e:
                        logging.error(
                            f"Failed inserting stat for job {job_id}, metric {metric}: {e}")

    logging.info(f"Inserted {inserted} stats for job {job_id}")


def load_zip(conn, zip_path):
    """
    Process a single ZIP file: extract, parse meta and data JSON, and insert into DB.
    """
    logging.info(f"Processing {zip_path}")
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

            if 'meta.json' not in names or 'data.json' not in names:
                logging.error(f"ZIP {zip_path} missing meta.json or data.json")
                return

            meta = json.load(zf.open('meta.json'))
            job_id = str(meta.get('jobId'))

            # Check if job already exists
            existing = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE id = ?", [job_id]).fetchone()[0]

            if existing > 0:
                logging.info(f"Job {job_id} already loaded, skipping")
                return

            # Insert job
            insert_job(conn, meta)

            # Process stats
            data_json = json.load(zf.open('data.json'))
            start_ts = datetime.fromtimestamp(meta.get('startTime', 0))

            insert_stats(conn, job_id, start_ts, data_json)

    except zipfile.BadZipFile:
        logging.error(f"Bad ZIP file: {zip_path}")

    except Exception as e:
        logging.error(f"Error processing {zip_path}: {e}")


def main():
    configure_logging()
    parser = argparse.ArgumentParser(
        description="Load HPC job data from zip JSON dumps into DuckDB"
    )
    parser.add_argument(
        '-f', '--database-file',
        help='DuckDB database file to write to', default="pika_jobs.duckdb"
    )
    parser.add_argument(
        'zips', nargs='+', help='Paths to ZIP files to import'
    )
    args = parser.parse_args()

    db_file = args.database_file

    conn = duckdb.connect(db_file)
    init_db(conn)

    for zip_path in args.zips:
        if not os.path.isfile(zip_path):
            logging.error(f"File not found: {zip_path}")
            continue
        load_zip(conn, zip_path)

    conn.close()
    logging.info("All done.")


if __name__ == '__main__':
    main()
