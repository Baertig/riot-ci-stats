#!/usr/bin/env python3

import requests
import json
import duckdb
import os
import sys
import logging
from datetime import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('riot_ci_stats')

# Database configuration
DB_FILE = "riot_ci_stats.duckdb"
CI_RIOT_URL = "https://ci.riot-os.org"

def create_database():
    """Create the database and schema if they don't exist"""
    conn = duckdb.connect(DB_FILE)
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
            state VARCHAR
        )
    ''')
    
    conn.execute("CREATE SEQUENCE seq_worker_stats START 1;")

    conn.execute('''
        CREATE TABLE IF NOT EXISTS worker_stats (
            id INTEGER PRIMARY KEY default nextval('seq_worker_stats'),
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
            FOREIGN KEY (job_uid) REFERENCES jobs(uid)
        )
    ''')

    
    conn.close()
    logger.info(f"Database initialized at {DB_FILE}")

def get_latest_job_date():
    """Get the latest job creation date from the database"""
    if not os.path.exists(DB_FILE):
        logger.info("Database does not exist yet, no latest date available")
        return None
    
    conn = duckdb.connect(DB_FILE)
    result = conn.execute("SELECT MAX(creation_time) FROM jobs").fetchone()
    conn.close()
    
    # Check if result is None or if the first element is None
    if result is None or result[0] is None:
        logger.info("No jobs found in database")
        return None
    else:
        latest_date = result[0]
        logger.info(f"Found latest job date in database: {latest_date}")
        # Format the date as YYYY-MM-DD
        return latest_date.strftime("%Y-%m-%d")

def remove_none_values(d):
    """Remove all keys with None values from a dictionary."""
    result = {}
    for key, value in d.items():
        if value is not None:
            result[key] = value
    return result

def fetch_jobs_data(limit = 25, status = [], after = None):
    """Fetch jobs data from the RIOT-OS CI server"""
    logger.info(f"Fetching job data from {CI_RIOT_URL}/jobs")

    params = {
        "limit": limit,
        "status": " ".join(status) if len(status) > 0 else None,
        "after": after,
    }

    params = remove_none_values(params)
    logger.info(f"with params: {params}")
    
    try:
        response = requests.get(CI_RIOT_URL + "/jobs", params=params)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        sys.exit(1)

def fetch_worker_stats(job_uid):
    """Fetch worker statistics for a specific job from the RIOT-OS CI server"""
    stats_url = f"{CI_RIOT_URL}/results/{job_uid}/stats.json"
    logger.info(f"Fetching worker statistics from {stats_url}")
    
    try:
        response = requests.get(stats_url)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.warning(f"Error fetching worker stats for job {job_uid}: {e}")
        return None

def insert_jobs_into_db(jobs_data):
    """Insert or update jobs in the database"""
    conn = duckdb.connect(DB_FILE)
    
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
        
        # Check if job already exists
        existing = conn.execute(f"SELECT uid FROM jobs WHERE uid = ?", [uid]).fetchone()
        
        if not existing:
            # Insert new job
            conn.execute('''
                INSERT INTO jobs (
                    uid, commit_sha, commit_message, commit_author,
                    creation_time, start_time, total_tasks_count, 
                    failed_tasks_count, passed_tasks_count, runtime, fetch_date, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                uid, commit_sha, commit_message, commit_author,
                datetime.fromtimestamp(creation_time), datetime.fromtimestamp(start_time),
                total_tasks_count, failed_tasks_count, passed_tasks_count, runtime,
                current_time, state
            ])
            inserted.append(uid)
    
    conn.close()
    return inserted 

def insert_worker_stats_into_db(job_uid, stats_data):
    """Insert worker statistics into the database"""
    if not stats_data or 'workers' not in stats_data or len(stats_data.get('workers')) == 0:
        logger.warning(f"No worker statistics found for job {job_uid}")
        return 0
    
    conn = duckdb.connect(DB_FILE)
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

def main():
    # Create database if it doesn't exist
    if not os.path.exists(DB_FILE):
        logger.info("Database did not exist ... creating database")
        create_database()
    
    # Get the latest job date from the database
    latest_date = get_latest_job_date()
    
    # Fetch jobs after the latest date in the database
    jobs_data = fetch_jobs_data(limit=50, status=["passed", "errored"], after=latest_date)
    
    if not jobs_data:
        logger.warning("No job data retrieved.")
        return
    
    # Insert jobs into database
    inserted_jobs_uid = insert_jobs_into_db(jobs_data)
    logger.info(f"Database updated: {len(inserted_jobs_uid)} new jobs inserted")
    
    # Fetch and insert worker statistics for each job
    stats_count = 0
    for uid in inserted_jobs_uid:
        # Check if worker stats already exist for this job
        conn = duckdb.connect(DB_FILE)
        existing_stats = conn.execute(
            "SELECT COUNT(*) FROM worker_stats WHERE job_uid = ?", 
            [uid]
        ).fetchone()[0]
        conn.close()
        
        if existing_stats > 0:
            logger.info(f"Worker stats already exist for job {uid}, skipping")
            continue
        
        # Fetch worker stats for this job
        stats_data = fetch_worker_stats(uid)
        
        if stats_data:
            inserted = insert_worker_stats_into_db(uid, stats_data)
            stats_count += inserted
            logger.info(f"Inserted {inserted} worker statistics for job {uid}")
        
        # Add a delay between requests to avoid overloading the server
        time.sleep(.2)  # 200ms delay between requests
    
    logger.info(f"Total worker statistics inserted: {stats_count}")

if __name__ == "__main__":
    main()