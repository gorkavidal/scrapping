#!/usr/bin/env python3
"""
Quick status checker for Google Maps Scraper.

Lightweight script designed for frequent status checks by AI assistants.
Outputs structured, parseable information about active and recent jobs.

Usage:
    python scraper_status.py [--json]
"""

import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path

# Auto-detect scraper path
SCRAPER_PATH = os.environ.get('SCRAPER_PATH')
if not SCRAPER_PATH:
    # Try relative path from script location
    script_dir = Path(__file__).parent.parent.parent
    if (script_dir / 'job_manager.py').exists():
        SCRAPER_PATH = str(script_dir)
    else:
        print("Error: SCRAPER_PATH not set and cannot auto-detect.")
        print("Set SCRAPER_PATH environment variable to the scrapping directory.")
        sys.exit(1)

sys.path.insert(0, SCRAPER_PATH)

from job_manager import JobManager, JobStatus, format_duration


def get_cookie_status() -> dict:
    """Check if cookies are valid."""
    cookies_file = Path(SCRAPER_PATH) / "browser_data" / "google_maps_state.json"

    if not cookies_file.exists():
        return {"valid": False, "message": "No cookies found - run --setup"}

    try:
        mtime = cookies_file.stat().st_mtime
        age_days = (time.time() - mtime) / (24 * 3600)

        if age_days > 7:
            return {
                "valid": False,
                "message": f"Cookies expired ({int(age_days)} days old) - run --setup",
                "age_days": int(age_days)
            }

        return {
            "valid": True,
            "message": f"Cookies valid ({int(age_days)} days old)",
            "age_days": round(age_days, 1)
        }
    except Exception as e:
        return {"valid": False, "message": f"Error checking cookies: {e}"}


def format_eta(stats) -> str:
    """Calculate estimated time remaining."""
    if stats.cities_processed == 0 or stats.cities_total == 0:
        return "Calculating..."

    avg_time = stats.runtime_seconds / stats.cities_processed
    remaining = stats.cities_total - stats.cities_processed
    eta_seconds = avg_time * remaining

    return format_duration(eta_seconds)


def get_status(output_json: bool = False) -> dict:
    """Get complete scraper status."""
    job_manager = JobManager(SCRAPER_PATH)

    # Clean up dead instances first
    job_manager.cleanup_dead_instances()

    result = {
        "timestamp": datetime.now().isoformat(),
        "cookies": get_cookie_status(),
        "active_jobs": [],
        "recent_history": [],
        "summary": {}
    }

    # Active instances
    instances = job_manager.list_instances()
    for inst in instances:
        stats = inst.stats
        config = inst.config

        job_info = {
            "job_id": inst.job_id,
            "pid": inst.pid,
            "status": inst.status,
            "query": config.get('query', 'N/A'),
            "country": config.get('country_code', 'N/A'),
            "workers": config.get('workers', 1),
            "cities_processed": stats.cities_processed,
            "cities_total": stats.cities_total,
            "progress_pct": round(stats.cities_processed / stats.cities_total * 100, 1) if stats.cities_total > 0 else 0,
            "results_with_email": stats.results_with_email,
            "results_corporate": stats.results_with_corporate_email,
            "runtime": format_duration(stats.runtime_seconds),
            "eta": format_eta(stats),
            "current_city": stats.current_city or "-"
        }
        result["active_jobs"].append(job_info)

    # Recent job history (last 5)
    jobs = job_manager.list_jobs()
    for job in jobs[:5]:
        stats = job.stats
        config = job.config

        history_info = {
            "job_id": job.job_id,
            "status": job.status,
            "query": config.get('query', 'N/A'),
            "country": config.get('country_code', 'N/A'),
            "cities_processed": stats.cities_processed,
            "cities_total": stats.cities_total,
            "results_with_email": stats.results_with_email,
            "runtime": format_duration(stats.runtime_seconds),
            "finished_at": job.finished_at[:16] if job.finished_at else "-",
            "can_resume": job.status in [JobStatus.INTERRUPTED, JobStatus.FAILED]
        }
        result["recent_history"].append(history_info)

    # Summary
    result["summary"] = {
        "active_count": len(result["active_jobs"]),
        "history_count": len(jobs),
        "total_emails_active": sum(j["results_with_email"] for j in result["active_jobs"]),
        "cookies_valid": result["cookies"]["valid"]
    }

    return result


def print_status(status: dict):
    """Print human-readable status."""
    print("=" * 60)
    print("GOOGLE MAPS SCRAPER STATUS")
    print("=" * 60)

    # Cookies
    cookies = status["cookies"]
    icon = "✓" if cookies["valid"] else "✗"
    print(f"\nCookies: {icon} {cookies['message']}")

    # Active jobs
    print(f"\n--- Active Jobs ({status['summary']['active_count']}) ---")
    if not status["active_jobs"]:
        print("No active scraping jobs.")
    else:
        for job in status["active_jobs"]:
            print(f"\n  [{job['status'].upper()}] {job['query']} in {job['country']}")
            print(f"    PID: {job['pid']} | Workers: {job['workers']}")
            print(f"    Progress: {job['cities_processed']}/{job['cities_total']} cities ({job['progress_pct']}%)")
            print(f"    Results: {job['results_with_email']} with email ({job['results_corporate']} corporate)")
            print(f"    Runtime: {job['runtime']} | ETA: {job['eta']}")
            if job['current_city'] != "-":
                print(f"    Current: {job['current_city']}")

    # History
    print(f"\n--- Recent History ({status['summary']['history_count']} total) ---")
    if not status["recent_history"]:
        print("No job history.")
    else:
        for job in status["recent_history"][:5]:
            status_icon = "●" if job["status"] == JobStatus.COMPLETED else "○"
            resume_hint = " [resumable]" if job["can_resume"] else ""
            print(f"  {status_icon} {job['query']} ({job['country']}) - {job['results_with_email']} emails - {job['status']}{resume_hint}")

    print("\n" + "=" * 60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Check Google Maps Scraper status")
    parser.add_argument('--json', action='store_true', help="Output as JSON")
    args = parser.parse_args()

    try:
        status = get_status()

        if args.json:
            print(json.dumps(status, indent=2, default=str))
        else:
            print_status(status)

    except Exception as e:
        if args.json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
