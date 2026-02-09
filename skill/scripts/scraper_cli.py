#!/usr/bin/env python3
"""
CLI for Google Maps Scraper control.

Provides commands to start, stop, pause, resume scraping jobs
and access results. Designed for use by AI assistants.

Usage:
    python scraper_cli.py start --query "dentistas" --country ES [options]
    python scraper_cli.py pause <job_id>
    python scraper_cli.py resume <job_id>
    python scraper_cli.py stop <job_id>
    python scraper_cli.py kill <pid>
    python scraper_cli.py results [--list] [job_id] [--summary] [--path]
"""

import os
import sys
import json
import signal
import subprocess
import argparse
import csv
from datetime import datetime
from pathlib import Path

# Auto-detect scraper path
SCRAPER_PATH = os.environ.get('SCRAPER_PATH')
if not SCRAPER_PATH:
    script_dir = Path(__file__).parent.parent.parent
    if (script_dir / 'job_manager.py').exists():
        SCRAPER_PATH = str(script_dir)
    else:
        print("Error: SCRAPER_PATH not set and cannot auto-detect.")
        sys.exit(1)

sys.path.insert(0, SCRAPER_PATH)

from job_manager import JobManager, JobStatus


def cmd_start(args):
    """Start a new scraping job."""
    # Validate required arguments
    if not args.query:
        print("Error: --query is required")
        return 1
    if not args.country:
        print("Error: --country is required")
        return 1

    # Resource check warning
    if args.workers > 2:
        print(f"Warning: Using {args.workers} workers requires significant RAM.")
        print("Consider using 1-2 workers for stability.")

    # Build command
    script_path = Path(SCRAPER_PATH) / "scrape_maps_interactive.py"
    cmd = [
        sys.executable, str(script_path),
        '--batch',
        '--country_code', args.country.upper(),
        '--query', args.query,
        '--min_population', str(args.min_pop),
        '--workers', str(args.workers),
        '--strategy', args.strategy,
        '--max_results', str(args.max_results),
    ]

    if args.max_pop:
        cmd.extend(['--max_population', str(args.max_pop)])
    if args.region:
        cmd.extend(['--region_code', args.region])

    # Log file
    log_file = Path(SCRAPER_PATH) / "scrapping_background.log"

    print(f"Starting scraping job:")
    print(f"  Query: {args.query}")
    print(f"  Country: {args.country.upper()}")
    print(f"  Population: {args.min_pop:,}+")
    print(f"  Workers: {args.workers}")
    print(f"  Strategy: {args.strategy}")

    try:
        with open(log_file, 'a') as lf:
            lf.write(f"\n{'='*60}\n")
            lf.write(f"New job started via CLI: {datetime.now().isoformat()}\n")
            lf.write(f"Command: {' '.join(cmd)}\n")
            lf.write(f"{'='*60}\n")

            proc = subprocess.Popen(
                cmd,
                stdout=lf,
                stderr=subprocess.STDOUT,
                start_new_session=True
            )

        print(f"\nJob started with PID: {proc.pid}")
        print(f"Log file: {log_file}")
        print("\nUse 'scraper_status.py' to monitor progress.")
        return 0

    except Exception as e:
        print(f"Error starting job: {e}")
        return 1


def cmd_pause(args):
    """Pause a running job."""
    job_manager = JobManager(SCRAPER_PATH)

    # Find the job
    target = args.job_id
    instance = None

    # Try as PID first
    try:
        pid = int(target)
        instance = job_manager.load_instance(pid)
    except ValueError:
        # Search by job_id
        for inst in job_manager.list_instances():
            if inst.job_id.startswith(target):
                instance = inst
                break

    if not instance:
        print(f"Error: No active job found matching '{target}'")
        return 1

    if instance.status == JobStatus.PAUSED:
        print(f"Job {instance.job_id} is already paused.")
        return 0

    # Send pause command
    if job_manager.send_command(instance.pid, 'pause'):
        print(f"Pause command sent to job {instance.job_id} (PID {instance.pid})")
        print("Job will pause after completing current batch (~10 businesses).")
        return 0
    else:
        print(f"Error sending pause command")
        return 1


def cmd_resume(args):
    """Resume a paused or interrupted job."""
    job_manager = JobManager(SCRAPER_PATH)
    target = args.job_id

    # Check if it's an active paused instance
    for inst in job_manager.list_instances():
        if str(inst.pid) == target or inst.job_id.startswith(target):
            if inst.status == JobStatus.PAUSED:
                if job_manager.send_command(inst.pid, 'resume'):
                    print(f"Resume command sent to job {inst.job_id}")
                    return 0
                else:
                    print("Error sending resume command")
                    return 1
            else:
                print(f"Job is not paused (status: {inst.status})")
                return 1

    # Check job history for interrupted jobs
    for job in job_manager.list_jobs():
        if job.job_id.startswith(target):
            if job.status not in [JobStatus.INTERRUPTED, JobStatus.FAILED]:
                print(f"Job cannot be resumed (status: {job.status})")
                return 1

            # Check for checkpoint
            if not job.checkpoint_file or not Path(job.checkpoint_file).exists():
                print("Error: No checkpoint file found for this job.")
                return 1

            # Resume with --resume flag
            script_path = Path(SCRAPER_PATH) / "scrape_maps_interactive.py"
            run_id = job.config.get('run_id')

            cmd = [sys.executable, str(script_path), '--resume', '--batch']
            if run_id:
                cmd.extend(['--run-id', run_id])

            log_file = Path(SCRAPER_PATH) / "scrapping_background.log"

            try:
                with open(log_file, 'a') as lf:
                    lf.write(f"\n{'='*60}\n")
                    lf.write(f"Resuming job: {job.job_id}\n")
                    lf.write(f"Timestamp: {datetime.now().isoformat()}\n")
                    lf.write(f"{'='*60}\n")

                    proc = subprocess.Popen(
                        cmd,
                        stdout=lf,
                        stderr=subprocess.STDOUT,
                        start_new_session=True
                    )

                # Delete old job entry to avoid duplicates
                job_manager.delete_job(job.job_id)

                print(f"Job resumed with PID: {proc.pid}")
                return 0

            except Exception as e:
                print(f"Error resuming job: {e}")
                return 1

    print(f"Error: No job found matching '{target}'")
    return 1


def cmd_stop(args):
    """Stop a running job gracefully."""
    job_manager = JobManager(SCRAPER_PATH)
    target = args.job_id

    for inst in job_manager.list_instances():
        if str(inst.pid) == target or inst.job_id.startswith(target):
            if job_manager.send_command(inst.pid, 'stop'):
                print(f"Stop command sent to job {inst.job_id} (PID {inst.pid})")
                print("Job will stop after completing current batch.")
                print("Job can be resumed later with 'resume' command.")
                return 0
            else:
                print("Error sending stop command")
                return 1

    print(f"Error: No active job found matching '{target}'")
    return 1


def cmd_kill(args):
    """Force kill a job immediately."""
    try:
        pid = int(args.pid)
        os.kill(pid, signal.SIGKILL)
        print(f"SIGKILL sent to PID {pid}")

        # Clean up instance
        job_manager = JobManager(SCRAPER_PATH)
        instance = job_manager.load_instance(pid)
        if instance:
            job = job_manager.load_job(instance.job_id)
            if job:
                job.status = JobStatus.INTERRUPTED
                job.finished_at = datetime.now().isoformat()
                job_manager.save_job(job)
            job_manager.unregister_instance(pid)

        print("Process killed. Job marked as interrupted and can be resumed.")
        return 0

    except ValueError:
        print("Error: kill requires a numeric PID")
        return 1
    except ProcessLookupError:
        print(f"Error: No process with PID {args.pid}")
        return 1
    except PermissionError:
        print(f"Error: Permission denied to kill PID {args.pid}")
        return 1


def cmd_results(args):
    """Access scraping results."""
    results_dir = Path(SCRAPER_PATH) / "scrappings"

    if args.list_all:
        # List all result files
        csv_files = sorted(results_dir.glob("results_*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)

        if not csv_files:
            print("No result files found.")
            return 0

        print(f"Result files ({len(csv_files)}):\n")
        for f in csv_files[:10]:  # Show last 10
            size = f.stat().st_size
            size_str = f"{size/1024:.1f}KB" if size < 1_000_000 else f"{size/1_000_000:.1f}MB"
            mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")

            # Count lines (approximate, fast)
            with open(f, 'r') as fh:
                lines = sum(1 for _ in fh) - 1

            print(f"  {f.name}")
            print(f"    {lines} records | {size_str} | {mtime}")

        return 0

    if args.job_id:
        # Find result file for specific job
        job_manager = JobManager(SCRAPER_PATH)

        # Search in history
        for job in job_manager.list_jobs():
            if job.job_id.startswith(args.job_id):
                if not job.csv_with_emails or not Path(job.csv_with_emails).exists():
                    print(f"No result file for job {job.job_id}")
                    return 1

                csv_path = Path(job.csv_with_emails)

                if args.path_only:
                    print(csv_path)
                    return 0

                if args.summary:
                    # Read CSV and show summary
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        rows = list(reader)

                    total = len(rows)
                    with_email = sum(1 for r in rows if r.get('Email_Raw', '').strip() or r.get('Email_Filtered', '').strip())
                    corporate = sum(1 for r in rows if r.get('Email_Filtered', '').strip())

                    print(f"Results for job: {job.job_id}")
                    print(f"  Query: {job.config.get('query')}")
                    print(f"  Country: {job.config.get('country_code')}")
                    print(f"  File: {csv_path.name}")
                    print(f"\nSummary:")
                    print(f"  Total businesses: {total}")
                    print(f"  With email: {with_email}")
                    print(f"  Corporate emails: {corporate}")
                    print(f"\nFile path: {csv_path}")
                    return 0

                # Default: show file info
                print(f"Result file: {csv_path}")
                print(f"Size: {csv_path.stat().st_size / 1024:.1f}KB")
                return 0

        # Also check active instances
        for inst in job_manager.list_instances():
            if inst.job_id.startswith(args.job_id):
                job = job_manager.load_job(inst.job_id)
                if job and job.csv_with_emails:
                    if args.path_only:
                        print(job.csv_with_emails)
                    else:
                        print(f"Result file (in progress): {job.csv_with_emails}")
                    return 0

        print(f"No job found matching '{args.job_id}'")
        return 1

    # Default: list files
    return cmd_results(argparse.Namespace(list_all=True, job_id=None, summary=False, path_only=False))


def main():
    parser = argparse.ArgumentParser(
        description="Google Maps Scraper CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Start command
    start_parser = subparsers.add_parser('start', help='Start new scraping job')
    start_parser.add_argument('--query', '-q', required=True, help='Business type to search')
    start_parser.add_argument('--country', '-c', required=True, help='Country code (ES, FR, US, etc.)')
    start_parser.add_argument('--min-pop', type=int, default=50000, help='Minimum city population')
    start_parser.add_argument('--max-pop', type=int, help='Maximum city population')
    start_parser.add_argument('--workers', '-w', type=int, default=1, help='Number of workers (1-3)')
    start_parser.add_argument('--strategy', choices=['simple', 'grid'], default='simple')
    start_parser.add_argument('--max-results', type=int, default=300, help='Max results per cell')
    start_parser.add_argument('--region', help='Region code to limit scope')

    # Pause command
    pause_parser = subparsers.add_parser('pause', help='Pause a running job')
    pause_parser.add_argument('job_id', help='Job ID or PID')

    # Resume command
    resume_parser = subparsers.add_parser('resume', help='Resume a paused/interrupted job')
    resume_parser.add_argument('job_id', help='Job ID or PID')

    # Stop command
    stop_parser = subparsers.add_parser('stop', help='Stop a running job gracefully')
    stop_parser.add_argument('job_id', help='Job ID or PID')

    # Kill command
    kill_parser = subparsers.add_parser('kill', help='Force kill a job immediately')
    kill_parser.add_argument('pid', help='Process ID')

    # Results command
    results_parser = subparsers.add_parser('results', help='Access results')
    results_parser.add_argument('job_id', nargs='?', help='Job ID to get results for')
    results_parser.add_argument('--list', dest='list_all', action='store_true', help='List all result files')
    results_parser.add_argument('--summary', action='store_true', help='Show summary of results')
    results_parser.add_argument('--path', dest='path_only', action='store_true', help='Show only file path')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    commands = {
        'start': cmd_start,
        'pause': cmd_pause,
        'resume': cmd_resume,
        'stop': cmd_stop,
        'kill': cmd_kill,
        'results': cmd_results,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
