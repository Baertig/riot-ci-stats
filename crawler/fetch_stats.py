#!/usr/bin/env python3

from prometheus_api_client import PrometheusConnect
import urllib.parse
import requests
import duckdb
import os
import sys
import logging
from datetime import datetime, timedelta
import time
import gzip
import io
import json
import argparse

CI_RIOT_URL = "https://ci.riot-os.org"
CI_STAGING_RIOT_URL = "https://ci-staging.riot-os.org"


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('riot_ci_stats')


def create_database(db_file):
    """Create the database and schema if they don't exist"""
    assert db_file, "database file path must be provided"
    conn = duckdb.connect(db_file)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            uid VARCHAR PRIMARY KEY,
            commit_sha VARCHAR,
            commit_message TEXT,
            commit_author VARCHAR,
            creation_time TIMESTAMP,
            start_time TIMESTAMP,
            total_tasks_count INTEGER,
            failed_tasks_count INTEGER,
            passed_tasks_count INTEGER,
            runtime DOUBLE,
            fetch_date TIMESTAMP,
            state VARCHAR,
            url VARCHAR
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS worker_stats (
            job_uid VARCHAR,
            name VARCHAR,
            tasks_count INTEGER,
            tasks_failed_count INTEGER,
            tasks_passed_count INTEGER,
            runtime_avg_s DOUBLE,
            runtime_max_s DOUBLE,
            runtime_min_s DOUBLE,
            total_cpu_time_s DOUBLE,
            fetch_date TIMESTAMP,
            FOREIGN KEY (job_uid) REFERENCES jobs(uid),
            PRIMARY KEY (job_uid, name)
        )
    ''')
    # create tasks_stats table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS tasks_stats (
            job_uid VARCHAR,
            worker_name VARCHAR,
            command VARCHAR,
            application VARCHAR,
            board VARCHAR,
            toolchain VARCHAR,
            runtime_s DOUBLE,
            created_at TIMESTAMP,
            state VARCHAR
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS redis_stats (
            redis_keyspace_hits_total_rate_1m DOUBLE,
            redis_keyspace_misses_total_rate_1m DOUBLE,
            process_network_receive_bytes_total_rate_1m DOUBLE,
            process_network_transmit_bytes_total_rate_1m DOUBLE,
            redis_evicted_keys_total_rate_1m DOUBLE,
            redis_expired_keys_total_rate_1m DOUBLE,
            redis_cpu_sys_seconds_total_rate_1m DOUBLE,
            redis_cpu_user_seconds_total_rate_1m DOUBLE,
            redis_memory_used_bytes_avg_1m DOUBLE,
            date_time TIMESTAMP,
            job_uid VARCHAR,
            FOREIGN KEY (job_uid) REFERENCES jobs(uid),
            PRIMARY KEY (job_uid, date_time)
        )
    ''')

    conn.close()
    logger.info(f"Database initialized at {db_file}")


def get_latest_job_date(url, db_file):
    """Get the latest job creation date from the database"""
    assert db_file, "database file path must be provided"
    if not os.path.exists(db_file):
        logger.info("Database does not exist yet, no latest date available")
        return None

    conn = duckdb.connect(db_file)
    result = conn.execute(
        "SELECT MAX(creation_time) FROM jobs WHERE url = ?", [url]).fetchone()
    conn.close()

    # Check if result is None or if the first element is None
    if result is None or result[0] is None:
        logger.info("No jobs found in database")
        return None
    else:
        latest_date = result[0]
        logger.info(
            f"Found latest job date in database from server {url}: {latest_date} ")

        return latest_date


def remove_none_values(d):
    """Remove all keys with None values from a dictionary."""
    result = {}
    for key, value in d.items():
        if value is not None:
            result[key] = value
    return result


def fetch_jobs_data(limit=25, status=[], after=None, url=CI_RIOT_URL):
    """Fetch jobs data from the RIOT-OS CI server"""
    logger.info(f"Fetching job data from {url}/jobs")

    params = {
        "limit": limit,
        "states": " ".join(status) if len(status) > 0 else None,
        "after": after.strftime("%Y-%m-%d"),
    }

    # I want spaces url encoded -> %20 and not encoded via +
    # otherwise the murdock api will ignore the filter
    params = urllib.parse.urlencode(
        remove_none_values(params), quote_via=urllib.parse.quote)
    logger.info(f"with params: {params}")

    try:
        response = requests.get(url + "/jobs", params=params)

        logger.info(f"send GET {response.url} request")

        response.raise_for_status()
        jobs = response.json()
        # attach base URL to each job entry
        for job in jobs:
            job['url'] = url
        return jobs

    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        sys.exit(1)


def fetch_worker_stats(job_uid, url=CI_RIOT_URL):
    """Fetch worker statistics for a specific job from the RIOT-OS CI server"""
    stats_url = f"{url}/results/{job_uid}/stats.json"
    logger.info(f"Fetching worker statistics from {stats_url}")

    try:
        response = requests.get(stats_url)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.warning(f"Error fetching worker stats for job {job_uid}: {e}")
        return None


def insert_jobs_into_db(jobs_data, db_file):
    """Insert or update jobs in the database"""
    conn = duckdb.connect(db_file)

    inserted = []

    current_time = datetime.now()

    for job in jobs_data:
        # Extract needed fields, using get() to handle missing keys
        uid = job.get('uid')
        if not uid:
            continue

        # Extract commit info
        commit_info = job.get('commit', {})
        commit_sha = commit_info.get('sha')
        commit_message = commit_info.get('message')
        commit_author = commit_info.get('author')

        # Get job timing data
        creation_time = job.get('creation_time')
        start_time = job.get('start_time')
        runtime = job.get('runtime')

        # Get status information
        status_info = job.get('status', {})
        total_tasks_count = status_info.get('total')
        failed_tasks_count = status_info.get('failed')
        passed_tasks_count = status_info.get('passed')

        state = job.get('state')
        url = job.get('url')

        # Check if job already exists
        existing = conn.execute(
            f"SELECT uid FROM jobs WHERE uid = ?", [uid]).fetchone()

        if not existing:
            # Insert new job
            conn.execute('''
                INSERT INTO jobs (
                    uid, commit_sha, commit_message, commit_author,
                    creation_time, start_time, total_tasks_count, 
                    failed_tasks_count, passed_tasks_count, runtime, fetch_date, state, url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                uid, commit_sha, commit_message, commit_author,
                datetime.fromtimestamp(
                    creation_time), datetime.fromtimestamp(start_time),
                total_tasks_count, failed_tasks_count, passed_tasks_count, runtime,
                current_time, state, url
            ])
            inserted.append(uid)

    conn.close()
    return inserted


def insert_worker_stats_into_db(job_uid, stats_data, db_file):
    """Insert worker statistics into the database"""
    assert db_file, "database file path must be provided"

    if not stats_data or 'workers' not in stats_data or len(stats_data.get('workers')) == 0:
        logger.warning(f"No worker statistics found for job {job_uid}")
        return 0

    conn = duckdb.connect(db_file)
    current_time = datetime.now()
    inserted = 0

    for worker in stats_data.get('workers'):
        # Extract needed fields
        name = worker.get('name')
        tasks_count = worker.get('jobs_count')
        tasks_failed_count = worker.get('jobs_failed')
        tasks_passed_count = worker.get('jobs_passed')
        runtime_avg_s = worker.get('runtime_avg')
        runtime_max_s = worker.get('runtime_max')
        runtime_min_s = worker.get('runtime_min')
        total_cpu_time_s = worker.get('total_cpu_time')

        # Insert worker stats
        conn.execute('''
            INSERT INTO worker_stats (
                job_uid, name, tasks_count, tasks_failed_count, tasks_passed_count,
                runtime_avg_s, runtime_max_s, runtime_min_s, total_cpu_time_s, fetch_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            job_uid, name, tasks_count, tasks_failed_count, tasks_passed_count,
            runtime_avg_s, runtime_max_s, runtime_min_s, total_cpu_time_s, current_time
        ])
        inserted += 1

    conn.close()
    return inserted


# fetch and insert task statistics functions

def fetch_task_stats(job_uid, url=CI_RIOT_URL):
    """Fetch task statistics for a specific job from the RIOT-OS CI server"""
    stats_url = f"{url}/results/{job_uid}/result.json.gz"
    logger.info(f"Fetching task statistics from {stats_url}")
    try:
        response = requests.get(stats_url)
        response.raise_for_status()
        buf = io.BytesIO(response.content)
        with gzip.GzipFile(fileobj=buf) as f:
            tasks = json.loads(f.read().decode())
        return tasks
    except requests.RequestException as e:
        logger.warning(f"Error fetching task stats for job {job_uid}: {e}")
    except (gzip.BadGzipFile, json.JSONDecodeError) as e:
        logger.warning(f"Error parsing task stats for job {job_uid}: {e}")
    return None


def insert_task_stats_into_db(job_uid, tasks_data, db_file):
    """Insert task statistics into the database"""
    created_at = datetime.now()

    if not tasks_data or len(tasks_data) == 0:
        logger.warning(f"No task statistics found for job {job_uid}")
        return 0

    conn = duckdb.connect(db_file)

    inserted = 0

    for task in tasks_data:
        worker_name = task.get('result', {}).get('worker')
        body = task.get('result', {}).get('body', {})
        command = body.get('command')
        state = task.get('state')

        application = None
        board = None
        toolchain = None
        if command:
            parts = command.split()
            if len(parts) >= 3:
                application = parts[2]

            if len(parts) >= 4:
                board_tool = parts[3].split(':')
                board = board_tool[0]

                if len(board_tool) > 1:
                    toolchain = board_tool[1]

        runtime = task.get('result', {}).get('runtime')

        conn.execute('''
            INSERT INTO tasks_stats (
                job_uid, worker_name, command, application, board, toolchain, runtime_s, created_at, state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [job_uid, worker_name, command, application, board, toolchain, runtime, created_at, state])

        inserted += 1

    conn.close()
    return inserted


def fetch_redis_data_from_prometheus(prometheus_host, job_uid, job_start_ts, job_runtime):
    """Fetch Redis metrics from Prometheus for the given job interval and return time series"""
    # Determine time range
    start_time = datetime.fromtimestamp(job_start_ts)
    end_time = start_time + timedelta(seconds=job_runtime)

    logger.info(
        f"Fetching Redis metrics for job {job_uid} ({start_time} to {end_time}) from Prometheus at {prometheus_host} ")
    try:
        prom = PrometheusConnect(url=prometheus_host, disable_ssl=True)
    except Exception as e:
        logger.error(
            f"Failed to connect to Prometheus at {prometheus_host}: {e}")
        return {}

    # Define metric queries
    metric_queries = {
        'redis_keyspace_hits_total_rate_1m': 'rate(redis_keyspace_hits_total[1m])',
        'redis_keyspace_misses_total_rate_1m': 'rate(redis_keyspace_misses_total[1m])',
        'process_network_receive_bytes_total_rate_1m': 'rate(process_network_receive_bytes_total[1m])',
        'process_network_transmit_bytes_total_rate_1m': 'rate(process_network_transmit_bytes_total[1m])',
        'redis_evicted_keys_total_rate_1m': 'rate(redis_evicted_keys_total[1m])',
        'redis_expired_keys_total_rate_1m': 'rate(redis_expired_keys_total[1m])',
        'redis_cpu_sys_seconds_total_rate_1m': 'rate(redis_cpu_sys_seconds_total[1m])',
        'redis_cpu_user_seconds_total_rate_1m': 'rate(redis_cpu_user_seconds_total[1m])',
        'redis_memory_used_bytes_avg_1m': 'avg_over_time(redis_memory_used_bytes[1m])'
    }

    results = {"date_time": []}
    for field, query in metric_queries.items():
        try:
            series = prom.custom_query_range(
                query=query,
                start_time=start_time,
                end_time=end_time,
                step='1m'
            )

            # take first series if exists
            if series and 'values' in series[0]:
                # list of [timestamp, value]
                values = map(lambda el: el[1], series[0]['values'])
                results[field] = list(values)

                # take the first non empty list of timestamps
                # timestamps should be the same for every fetched series
                # This way we can be sure date_time list exist if at least on
                # metric was found
                if len(results["date_time"]) == 0:
                    timestamps = map(lambda el: el[0], series[0]['values'])
                    results["date_time"] = list(timestamps)
            else:
                logger.warning(f"No series returned for query '{query}'")
                results[field] = []

        except Exception as e:
            logger.error(f"Error fetching metric '{field}': {e}")
            results[field] = []

    return results


def insert_redis_metrics_into_db(job_uid, results, db_file):
    """Insert fetched Redis metrics time series into DuckDB"""

    if not results or len(results["date_time"]) == 0:
        logger.warning(f"No Redis metrics to insert for job {job_uid}")
        return 0

    # Determine metric fields and timestamps
    fields = [key for key in results.keys() if key != 'date_time']
    timestamps = results.get('date_time')

    rows = []
    for idx, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts)
        values = []
        for field in fields:
            series = results.get(field, [])
            # It could be that some metric are missing
            # e.g. redis-exporter lost connection to the redis instance
            val = float(series[idx]) if len(series) > idx else None

            values.append(val)

        # build row: metrics..., date_time, job_uid
        rows.append(tuple(values + [dt, job_uid]))

    # Prepare insert SQL

    cols = ", ".join(fields + ["date_time", "job_uid"])
    placeholder_count = len(fields) + 2
    placeholders = ", ".join("?" for _ in range(placeholder_count))

    sql = f"INSERT INTO redis_stats ({cols}) VALUES ({placeholders})"
    try:
        with duckdb.connect(db_file) as conn:
            conn.executemany(sql, rows)
        logger.info(f"Inserted {len(rows)} Redis stats rows for job {job_uid}")
        return len(rows)
    except Exception as e:
        logger.error(
            f"Error inserting Redis stats into database for job {job_uid}: {e}")
        return 0


def to_end_of_previous_day(date):

    previous_day = date - timedelta(days=1)
    end_of_previous_day = previous_day.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=999_999
    )

    return end_of_previous_day


def main():
    parser = argparse.ArgumentParser(
        description="Fetch and store CI stats from murdock and redis into DuckDB")

    parser.add_argument('-f', '--database-file', required=True,
                        help='Path to DuckDB database file')

    parser.add_argument('-p', '--prometheus-host', required=True,
                        help='Path to DuckDB database file')

    args = parser.parse_args()

    db_file = args.database_file

    # Create database if it doesn't exist
    if not os.path.exists(db_file):
        logger.info("Database did not exist ... creating database")
        create_database(db_file)

    # Get the latest job date from the database
    latest_date_prod = get_latest_job_date(CI_RIOT_URL, db_file)

    if not latest_date_prod:
        latest_date_prod = datetime.now() - timedelta(days=30)

    latest_date_staging = get_latest_job_date(CI_STAGING_RIOT_URL, db_file)

    if not latest_date_staging:
        latest_date_staging = datetime.now() - timedelta(days=30)

    # Can only input date at day resolution to API
    # ... this way we don't miss jobs that executed on the same day, but after the latest job
    latest_date_prod = to_end_of_previous_day(latest_date_prod)
    latest_date_staging = to_end_of_previous_day(latest_date_staging)

    # Fetch jobs after the latest date in the database
    jobs_data_prod = fetch_jobs_data(
        limit=50, status=["passed", "errored"], after=latest_date_prod, url=CI_RIOT_URL)
    jobs_data_staging = fetch_jobs_data(limit=50, status=[
                                        "passed", "errored"], after=latest_date_staging, url=CI_STAGING_RIOT_URL)

    if not jobs_data_prod:
        logger.warning("No job data retrieved from prod")

    if not jobs_data_staging:
        logger.warning("No job data retrieved from staging exiting")

    jobs_data = jobs_data_prod + jobs_data_staging

    if not jobs_data:
        return

    # Insert jobs into database
    inserted_jobs_uid = insert_jobs_into_db(jobs_data, db_file)
    logger.info(
        f"Database updated: {len(inserted_jobs_uid)} new jobs inserted")

    # Fetch and insert worker statistics for each job
    stats_count = 0
    tasks_count = 0
    redis_stats_count = 0

    conn = duckdb.connect(db_file)
    for uid in inserted_jobs_uid:
        # Check if worker stats already exist for this job
        existing_stats = conn.execute(
            "SELECT COUNT(*) FROM worker_stats WHERE job_uid = ?",
            [uid]
        ).fetchone()[0]

        if existing_stats > 0:
            logger.info(f"Worker stats already exist for job {uid}, skipping")
            continue

        # Fetch worker stats for this job
        # pass along the job's base URL when fetching stats
        job = next((j for j in jobs_data if j.get('uid') == uid))

        job_url = job.get('url')

        if not job_url:
            logger.warning(
                f"Did not find url for job with uid {uid}. Skipping")
            continue

        stats_data = fetch_worker_stats(uid, job_url)

        if stats_data:
            inserted = insert_worker_stats_into_db(uid, stats_data, db_file)
            stats_count += inserted
            logger.info(f"Inserted {inserted} worker statistics for job {uid}")

        # Fetch and insert task statistics
        if job_url == CI_STAGING_RIOT_URL:
            task_stats = fetch_task_stats(uid, job_url)
            if task_stats:
                inserted_tasks = insert_task_stats_into_db(
                    uid, task_stats, db_file)
                tasks_count += inserted_tasks
                logger.info(
                    f"Inserted {inserted_tasks} task statistics for job {uid}")

            # fetch redis data from prometheus
            redis_series = fetch_redis_data_from_prometheus(
                args.prometheus_host,
                uid,
                job.get('start_time'),
                job.get('runtime')
            )

            redis_stats_count += insert_redis_metrics_into_db(
                uid,
                redis_series,
                db_file
            )

        # Add a delay between requests to avoid overloading the server
        time.sleep(.2)  # 200ms delay between requests

    conn.close()
    logger.info(f"Total worker statistics inserted: {stats_count}")
    logger.info(f"Total task statistics inserted: {tasks_count}")
    logger.info(f"Total redis statistics inserted: {redis_stats_count}")




if __name__ == "__main__":
    main()
