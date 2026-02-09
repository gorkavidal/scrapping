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
    python scraper_cli.py results [--list] [job_id] [--summary] [--path] [--show N] [--format FORMAT]
    python scraper_cli.py files [--output-dir]
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


def get_output_dir() -> Path:
    """Get the output directory for results.

    Priority:
    1. SCRAPER_OUTPUT_DIR environment variable
    2. Default: $SCRAPER_PATH/scrappings
    """
    output_dir = os.environ.get('SCRAPER_OUTPUT_DIR')
    if output_dir:
        return Path(output_dir)
    return Path(SCRAPER_PATH) / "scrappings"


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
    if args.output_dir:
        cmd.extend(['--output_dir', args.output_dir])

    # Log file
    log_file = Path(SCRAPER_PATH) / "scrapping_background.log"

    # Determine output directory for display
    output_dir = args.output_dir or get_output_dir()

    print(f"Starting scraping job:")
    print(f"  Query: {args.query}")
    print(f"  Country: {args.country.upper()}")
    print(f"  Population: {args.min_pop:,}+")
    print(f"  Workers: {args.workers}")
    print(f"  Strategy: {args.strategy}")
    print(f"  Output dir: {output_dir}")

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


def count_csv_records(filepath: Path) -> int:
    """Count records in CSV using csv module (handles multiline fields)."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            return sum(1 for _ in reader)
    except:
        return 0


def read_csv_records(filepath: Path) -> list:
    """Read all records from CSV file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []


def format_record_table(row: dict, index: int) -> str:
    """Format a single record for display."""
    name = row.get('Name', '')[:45]
    city = row.get('Localidad', '')
    region = row.get('Region', '')
    email_raw = row.get('Email_Raw', '')
    email_corp = row.get('Email_Filtered', '')
    web = row.get('Website', '')[:50] if row.get('Website') else '-'
    phone = row.get('Phone', '')

    email = email_corp if email_corp else email_raw
    corp_mark = '★' if email_corp else ' '

    lines = [f"{index:3}. {corp_mark} {name}"]
    lines.append(f"      📍 {city}, {region}")
    if email:
        # Truncate long email lists
        email_display = email[:70] + '...' if len(email) > 70 else email
        lines.append(f"      📧 {email_display}")
    if web != '-':
        lines.append(f"      🌐 {web}")
    if phone:
        lines.append(f"      📞 {phone}")

    return '\n'.join(lines)


def format_record_csv_line(row: dict) -> str:
    """Format record as CSV-like line for easy copying."""
    return f"{row.get('Name', '')}\t{row.get('Email_Filtered') or row.get('Email_Raw', '')}\t{row.get('Phone', '')}\t{row.get('Website', '')}"


def format_record_json(row: dict) -> dict:
    """Format record as JSON-friendly dict."""
    return {
        'name': row.get('Name', ''),
        'city': row.get('Localidad', ''),
        'region': row.get('Region', ''),
        'email': row.get('Email_Filtered') or row.get('Email_Raw', ''),
        'email_corporate': row.get('Email_Filtered', ''),
        'phone': row.get('Phone', ''),
        'website': row.get('Website', ''),
        'address': row.get('Address', ''),
        'rating': row.get('Rating', '')
    }


def cmd_results(args):
    """Access scraping results."""
    results_dir = get_output_dir()

    # Ensure directory exists
    if not results_dir.exists():
        print(f"Output directory does not exist: {results_dir}")
        print(f"Set SCRAPER_OUTPUT_DIR or use default at $SCRAPER_PATH/scrappings")
        return 1

    # --list: List all result files
    if args.list_all:
        csv_files = sorted(results_dir.glob("results_*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)

        if not csv_files:
            print(f"No result files found in {results_dir}")
            return 0

        print(f"Result files in {results_dir}:")
        print(f"({len(csv_files)} total, showing last 15)\n")

        for f in csv_files[:15]:
            size = f.stat().st_size
            size_str = f"{size/1024:.1f}KB" if size < 1_000_000 else f"{size/1_000_000:.1f}MB"
            mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            records = count_csv_records(f)

            print(f"  {f.name}")
            print(f"    {records} records | {size_str} | {mtime}")

        # Also show no_emails files count
        no_email_files = list(results_dir.glob("no_emails_*.csv"))
        if no_email_files:
            print(f"\n  (+ {len(no_email_files)} no_emails_*.csv files)")

        print(f"\nOutput directory: {results_dir}")
        return 0

    # If job_id specified, find that job's results
    if args.job_id:
        job_manager = JobManager(SCRAPER_PATH)
        job = None
        csv_path = None

        # Search in job history
        for j in job_manager.list_jobs():
            if j.job_id.startswith(args.job_id):
                job = j
                if j.csv_with_emails and Path(j.csv_with_emails).exists():
                    csv_path = Path(j.csv_with_emails)
                break

        # Also check active instances
        if not job:
            for inst in job_manager.list_instances():
                if inst.job_id.startswith(args.job_id):
                    job = job_manager.load_job(inst.job_id)
                    if job and job.csv_with_emails:
                        csv_path = Path(job.csv_with_emails)
                    break

        if not job:
            print(f"No job found matching '{args.job_id}'")
            return 1

        if not csv_path or not csv_path.exists():
            print(f"No result file for job {job.job_id}")
            print(f"Job status: {job.status}")
            return 1

        # --path: Just show the path
        if args.path_only:
            print(csv_path)
            return 0

        # Read the CSV
        rows = read_csv_records(csv_path)

        # Calculate stats
        total = len(rows)
        with_email = sum(1 for r in rows if r.get('Email_Raw', '').strip() or r.get('Email_Filtered', '').strip())
        corporate = sum(1 for r in rows if r.get('Email_Filtered', '').strip())

        # Get no_emails file if exists
        no_emails_file = csv_path.parent / csv_path.name.replace('results_', 'no_emails_')
        no_emails_count = count_csv_records(no_emails_file) if no_emails_file.exists() else 0

        # --summary: Show summary stats
        if args.summary:
            print(f"Results for job: {job.job_id}")
            print(f"  Query: {job.config.get('query')}")
            print(f"  Country: {job.config.get('country_code')}")
            print(f"  Status: {job.status}")
            print()
            print("Files generated:")
            print(f"  📄 Results (with email): {csv_path.name}")
            print(f"     → {total} businesses, {corporate} corporate emails")
            if no_emails_file.exists():
                print(f"  📄 No emails: {no_emails_file.name}")
                print(f"     → {no_emails_count} businesses without email")
            print()
            print("Summary:")
            print(f"  Total businesses scraped: {total + no_emails_count}")
            print(f"  With any email: {with_email} ({with_email*100//(total+no_emails_count) if total+no_emails_count > 0 else 0}%)")
            print(f"  Corporate emails: {corporate} ({corporate*100//with_email if with_email > 0 else 0}% of emails)")
            print(f"  Without email: {no_emails_count}")
            print()
            print(f"File path: {csv_path}")
            if no_emails_file.exists():
                print(f"No-email file: {no_emails_file}")
            return 0

        # --show N: Show last N records
        if args.show:
            n = min(args.show, len(rows))
            records_to_show = rows[-n:] if n < len(rows) else rows

            print(f"Last {n} results from job {job.job_id}:")
            print(f"(Query: {job.config.get('query')} in {job.config.get('country_code')})")
            print("=" * 60)

            # Format based on --format
            if args.format == 'json':
                output = [format_record_json(r) for r in records_to_show]
                print(json.dumps(output, indent=2, ensure_ascii=False))
            elif args.format == 'tsv':
                print("Name\tEmail\tPhone\tWebsite")
                for r in records_to_show:
                    print(format_record_csv_line(r))
            else:  # table (default)
                for i, row in enumerate(records_to_show, len(rows) - n + 1):
                    print(format_record_table(row, i))
                    print()

            print("=" * 60)
            print(f"Showing {n} of {total} records")
            print(f"★ = Corporate email (matches website domain)")
            return 0

        # Default: show basic info + last 5 records
        print(f"Results for job: {job.job_id}")
        print(f"  Query: {job.config.get('query')}")
        print(f"  Country: {job.config.get('country_code')}")
        print(f"  Total: {total} records with email")
        print(f"  Corporate: {corporate}")
        print()
        print(f"File: {csv_path}")
        print(f"Size: {csv_path.stat().st_size / 1024:.1f}KB")

        if total > 0:
            print()
            print("Last 5 records:")
            print("-" * 50)
            for i, row in enumerate(rows[-5:], max(1, total - 4)):
                print(format_record_table(row, i))
                print()

        print(f"\nUse --show N to see more records, --summary for stats")
        return 0

    # No job_id: list files by default
    return cmd_results(argparse.Namespace(
        list_all=True, job_id=None, summary=False,
        path_only=False, show=None, format='table'
    ))


def cmd_files(args):
    """Show information about output files and directories."""
    results_dir = get_output_dir()
    cache_dir = Path(SCRAPER_PATH) / "cache"
    checkpoints_dir = Path(SCRAPER_PATH) / "checkpoints"
    browser_dir = Path(SCRAPER_PATH) / "browser_data"

    print("Google Maps Scraper - File Locations")
    print("=" * 60)

    print("\n📁 Output Directory (results):")
    print(f"   {results_dir}")
    if results_dir.exists():
        csv_count = len(list(results_dir.glob("*.csv")))
        total_size = sum(f.stat().st_size for f in results_dir.glob("*.csv"))
        print(f"   {csv_count} CSV files, {total_size/1_000_000:.1f}MB total")
    else:
        print("   (does not exist)")

    print("\n📁 Cache Directory (jobs, control):")
    print(f"   {cache_dir}")

    print("\n📁 Checkpoints Directory (resume data):")
    print(f"   {checkpoints_dir}")
    if checkpoints_dir.exists():
        ckpt_count = len(list(checkpoints_dir.glob("*.json")))
        print(f"   {ckpt_count} checkpoint files")

    print("\n📁 Browser Data (cookies):")
    print(f"   {browser_dir}")
    cookies_file = browser_dir / "google_maps_state.json"
    if cookies_file.exists():
        import time
        age_days = (time.time() - cookies_file.stat().st_mtime) / (24 * 3600)
        status = "valid" if age_days < 7 else "EXPIRED"
        print(f"   Cookies: {status} ({age_days:.1f} days old)")
    else:
        print("   Cookies: NOT FOUND (run --setup)")

    print("\n" + "=" * 60)
    print("\nFile types generated per job:")
    print("  results_<COUNTRY>_<QUERY>_<ID>_<DATE>.csv")
    print("    → Businesses WITH email found")
    print("  no_emails_<COUNTRY>_<QUERY>_<ID>_<DATE>.csv")
    print("    → Businesses WITHOUT email (have website)")
    print()
    print("Configure output directory:")
    print("  export SCRAPER_OUTPUT_DIR=\"/path/to/output\"")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Google Maps Scraper CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s start --query "dentistas" --country ES
  %(prog)s results --list
  %(prog)s results job_2026 --summary
  %(prog)s results job_2026 --show 20
  %(prog)s results job_2026 --show 10 --format json
  %(prog)s files
"""
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
    start_parser.add_argument('--output-dir', '-o', help='Output directory for results')

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
    results_parser = subparsers.add_parser('results', help='Access and view results')
    results_parser.add_argument('job_id', nargs='?', help='Job ID to get results for')
    results_parser.add_argument('--list', dest='list_all', action='store_true', help='List all result files')
    results_parser.add_argument('--summary', action='store_true', help='Show detailed summary with stats')
    results_parser.add_argument('--path', dest='path_only', action='store_true', help='Show only file path (for scripting)')
    results_parser.add_argument('--show', type=int, metavar='N', help='Show last N records')
    results_parser.add_argument('--format', choices=['table', 'json', 'tsv'], default='table',
                               help='Output format for --show (default: table)')

    # Files command
    files_parser = subparsers.add_parser('files', help='Show file locations and output directory info')

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
        'files': cmd_files,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
