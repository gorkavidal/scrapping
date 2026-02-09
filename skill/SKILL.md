---
name: gmaps-scraper
description: Orchestrates Google Maps business scraping campaigns with email extraction. Use when the user wants to find businesses in specific locations, extract contact information from Google Maps, run lead generation campaigns, or check the status of ongoing scraping jobs. Triggers on keywords like "scrape", "buscar negocios", "extraer emails", "Google Maps", "lead generation", "scraping status".
metadata:
  version: 1.1.0
  author: Gorka Vidal
  category: data-extraction
  domain: lead-generation
  repository: https://github.com/gorkavidal/scrapping
allowed-tools: [Read, Write, Bash, Glob, Grep]
---

# Google Maps Business Scraper

Orchestrates automated extraction of business information from Google Maps, including emails from websites. Designed for lead generation campaigns with a focus on resource efficiency and reliability.

## Overview

This skill enables conversational control of a powerful Google Maps scraping system that:
- Searches businesses by query and geographic area
- Extracts contact details (phone, website, address)
- Crawls websites to find email addresses
- Supports multi-city campaigns with population filters
- Runs in background with checkpoint/resume capability

## When to Use This Skill

- User wants to find businesses of a specific type in a country/region
- User needs to extract emails from Google Maps listings
- User asks about progress of an ongoing scraping campaign
- User wants to pause, resume, or stop a scraping job
- User needs to export or analyze scraping results
- User asks "show me the last N results" or "where are the files?"

## Installation

The skill requires the scraper repository to be cloned and dependencies installed.

### Using uv (recommended)

**Linux/macOS:**
```bash
# Install uv
# macOS: brew install uv
# Linux: curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and setup
git clone https://github.com/gorkavidal/scrapping.git
cd scrapping
uv venv
uv pip install -r requirements.txt

# Install Playwright browsers
.venv/bin/python -m playwright install chromium
# Linux only (install system dependencies):
# .venv/bin/python -m playwright install-deps chromium
```

**Windows (PowerShell):**
```powershell
# Install uv
irm https://astral.sh/uv/install.ps1 | iex

# Clone and setup
git clone https://github.com/gorkavidal/scrapping.git
cd scrapping
uv venv
uv pip install -r requirements.txt

# Install Playwright browsers
.venv\Scripts\python.exe -m playwright install chromium
```

### Using pip (alternative)

**Linux/macOS:**
```bash
git clone https://github.com/gorkavidal/scrapping.git
cd scrapping

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

**Windows (PowerShell):**
```powershell
git clone https://github.com/gorkavidal/scrapping.git
cd scrapping

python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
```

### Environment Configuration

**Set the SCRAPER_PATH environment variable** (add to ~/.bashrc or ~/.zshrc):
```bash
export SCRAPER_PATH="/path/to/scrapping"
```

**Optional: Configure custom output directory:**
```bash
export SCRAPER_OUTPUT_DIR="/path/to/results"
```

**Optional: Custom GeoNames username** (if you experience rate limiting with the default `gorkota` user):
1. Create a free account at https://www.geonames.org/login
2. Enable free web services at https://www.geonames.org/manageaccount
3. Set the environment variable or use `.env`:
```bash
export GEONAMES_USERNAME="your_geonames_username"
```

**Alternative:** Copy `.env.example` to `.env` and fill in your values.

## Running Scripts

Scripts can be executed in multiple ways depending on your setup:

### Using uv (recommended)
```bash
# From the scraper directory
cd "$SCRAPER_PATH"
uv run skill/scripts/scraper_status.py
uv run skill/scripts/scraper_cli.py start --query "dentistas" --country ES
```

### Using .venv directly (Linux/macOS)
```bash
"$SCRAPER_PATH/.venv/bin/python" "$SCRAPER_PATH/skill/scripts/scraper_status.py"
"$SCRAPER_PATH/.venv/bin/python" "$SCRAPER_PATH/skill/scripts/scraper_cli.py" start --query "dentistas" --country ES
```

### Using .venv directly (Windows PowerShell)
```powershell
& "$env:SCRAPER_PATH\.venv\Scripts\python.exe" "$env:SCRAPER_PATH\skill\scripts\scraper_status.py"
& "$env:SCRAPER_PATH\.venv\Scripts\python.exe" "$env:SCRAPER_PATH\skill\scripts\scraper_cli.py" start --query "dentistas" --country ES
```

### Using activated venv
```bash
# First activate the environment
source "$SCRAPER_PATH/.venv/bin/activate"  # Linux/macOS
# or: & "$env:SCRAPER_PATH\.venv\Scripts\Activate.ps1"  # Windows PowerShell

# Then run scripts normally
python "$SCRAPER_PATH/skill/scripts/scraper_status.py"
```

**Note:** The examples below use `uv run` syntax. Replace with your preferred method.

## Quick Reference

### Check Status (Most Common)
```bash
cd "$SCRAPER_PATH" && uv run skill/scripts/scraper_status.py
```

### Start New Scraping
```bash
cd "$SCRAPER_PATH" && uv run skill/scripts/scraper_cli.py start \
  --query "dentistas" --country ES --min-pop 50000 --workers 1
```

### Control Jobs
```bash
cd "$SCRAPER_PATH"
uv run skill/scripts/scraper_cli.py pause <job_id>
uv run skill/scripts/scraper_cli.py resume <job_id>
uv run skill/scripts/scraper_cli.py stop <job_id>
```

### View Results
```bash
cd "$SCRAPER_PATH"

# List all result files
uv run skill/scripts/scraper_cli.py results --list

# Show last 20 records from a job
uv run skill/scripts/scraper_cli.py results <job_id> --show 20

# Get detailed summary
uv run skill/scripts/scraper_cli.py results <job_id> --summary

# Get file path only (for scripting)
uv run skill/scripts/scraper_cli.py results <job_id> --path

# Export as JSON
uv run skill/scripts/scraper_cli.py results <job_id> --show 50 --format json

# Show file locations
uv run skill/scripts/scraper_cli.py files
```

## Resource Management Principles

**CRITICAL: This scraper must be resource-conservative.**

1. **Default to 1-2 workers maximum** - More workers = more browser instances = more RAM
2. **Never suggest more than 3 workers** unless user explicitly requests and has 16GB+ RAM
3. **Always check system resources before starting** new jobs
4. **Recommend pausing** if system is under load
5. **Batch processing** - Results save every 10 businesses, safe to interrupt anytime

### Resource Guidelines

| System RAM | Max Workers | Concurrent Jobs |
|------------|-------------|-----------------|
| 8GB        | 1           | 1               |
| 16GB       | 2           | 1               |
| 32GB+      | 3           | 2               |

## Core Workflow

### 1. First-Time Setup (Required)

Before any scraping, Google Maps cookies must be obtained:

```bash
# Using uv (recommended)
cd "$SCRAPER_PATH" && uv run scrape_maps_interactive.py --setup

# Or using .venv directly (Linux/macOS)
"$SCRAPER_PATH/.venv/bin/python" "$SCRAPER_PATH/scrape_maps_interactive.py" --setup

# Windows PowerShell
& "$env:SCRAPER_PATH\.venv\Scripts\python.exe" "$env:SCRAPER_PATH\scrape_maps_interactive.py" --setup
```

This opens a visible browser. User must:
1. Accept Google cookies/CAPTCHA if prompted
2. Navigate to Maps and do a manual search
3. Close browser when done

**Cookies are valid for ~7 days.** After expiration, run setup again.

### 2. Starting a Campaign

**Conservative approach (recommended for most users):**
```bash
cd "$SCRAPER_PATH" && uv run skill/scripts/scraper_cli.py start \
  --query "clinicas dentales" \
  --country ES \
  --min-pop 100000 \
  --workers 1 \
  --strategy simple
```

**With custom output directory:**
```bash
cd "$SCRAPER_PATH" && uv run skill/scripts/scraper_cli.py start \
  --query "clinicas dentales" \
  --country ES \
  --output-dir /path/to/results
```

**Parameters:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| `--query` | Business type to search (local language works best) | Required |
| `--country` | ISO 2-letter code (ES, FR, US, DE, etc.) | Required |
| `--min-pop` | Minimum city population | 50000 |
| `--max-pop` | Maximum city population (optional) | None |
| `--workers` | Parallel browser instances | 1 |
| `--strategy` | `simple` or `grid` (for dense areas) | simple |
| `--region` | Region code to limit scope (optional) | None |
| `--output-dir` | Custom output directory for results | $SCRAPER_OUTPUT_DIR or scrappings/ |

### 3. Monitoring Progress

**Quick status check (use this most often):**
```bash
cd "$SCRAPER_PATH" && uv run skill/scripts/scraper_status.py
```

Output shows:
- Active jobs with PID, cities processed, emails found
- Runtime and estimated time remaining
- Recent job history with results

**Interactive TUI manager (advanced):**
```bash
cd "$SCRAPER_PATH" && uv run scrape_manager.py
```

### 4. Controlling Jobs

```bash
cd "$SCRAPER_PATH"

# Pause - saves checkpoint, frees system resources
uv run skill/scripts/scraper_cli.py pause <job_id>

# Resume - continues from last checkpoint
uv run skill/scripts/scraper_cli.py resume <job_id>

# Stop gracefully - finishes current batch, then stops
uv run skill/scripts/scraper_cli.py stop <job_id>

# Force kill - immediate stop (may lose current city progress)
uv run skill/scripts/scraper_cli.py kill <pid>
```

### 5. Accessing Results

Results are saved incrementally to CSV files.

```bash
cd "$SCRAPER_PATH"

# List all result files with record counts
uv run skill/scripts/scraper_cli.py results --list

# Get detailed summary of a job (files, stats, paths)
uv run skill/scripts/scraper_cli.py results <job_id> --summary

# Show last N records in table format
uv run skill/scripts/scraper_cli.py results <job_id> --show 20

# Show records in JSON format (for processing)
uv run skill/scripts/scraper_cli.py results <job_id> --show 50 --format json

# Show records in TSV format (for spreadsheets)
uv run skill/scripts/scraper_cli.py results <job_id> --show 100 --format tsv

# Get only the file path (for scripting)
uv run skill/scripts/scraper_cli.py results <job_id> --path

# Show all file locations and directory info
uv run skill/scripts/scraper_cli.py files
```

## Output Files

### Files Generated Per Job

Each scraping job generates two CSV files:

| File Pattern | Content | Description |
|--------------|---------|-------------|
| `results_<COUNTRY>_<QUERY>_<ID>_<DATE>.csv` | Businesses WITH email | Main results file |
| `no_emails_<COUNTRY>_<QUERY>_<ID>_<DATE>.csv` | Businesses WITHOUT email | Have website but no email found |

**Example:**
```
results_ES_dentistas_a1b2c3d4_20260209_143052.csv
no_emails_ES_dentistas_a1b2c3d4_20260209_143052.csv
```

### CSV Columns

| Column | Description |
|--------|-------------|
| `Name` | Business name |
| `Localidad` | City/locality |
| `Region` | State/province/region |
| `Address` | Full address |
| `Phone` | Phone number |
| `Rating` | Google Maps rating |
| `Website` | Business website URL |
| `Email_Raw` | All emails found on website |
| `Email_Filtered` | Corporate emails only (matching website domain) |
| `Email_Search_Status` | Email extraction status |
| `ID` | Unique identifier |

### Output Directory Configuration

Priority order for output directory:
1. `--output-dir` argument (per-job)
2. `SCRAPER_OUTPUT_DIR` environment variable
3. Default: `$SCRAPER_PATH/scrappings/`

```bash
# Set custom output directory globally
export SCRAPER_OUTPUT_DIR="/home/user/leads/gmaps"

# Or per-job
cd "$SCRAPER_PATH" && uv run skill/scripts/scraper_cli.py start \
  --query "dentistas" --country ES --output-dir /tmp/test
```

## Conversation Patterns

### User asks: "How's the scraping going?" / "Status del scraping"

1. Run `scraper_status.py`
2. Report: active jobs, cities processed/total, emails found, ETA
3. If no active jobs, mention recent completions from history

### User asks: "Start scraping [business type] in [location]"

1. Check if cookies are valid (warn if > 7 days old)
2. Parse location -> country code + optional region
3. Suggest 1 worker (conservative default)
4. Start job with `scraper_cli.py start`
5. Confirm job started, provide job_id

### User asks: "Stop/Pause the scraping"

1. Get list of active jobs via status
2. If multiple, ask which one
3. Execute pause/stop command
4. Confirm action, mention resume is possible

### User asks: "Show me the results" / "Dame los resultados"

1. Find the relevant job (latest or specified)
2. Run `results <job_id> --show 20` to show recent records
3. Provide summary stats (total, with email, corporate)
4. Show the file path for full access

### User asks: "Show me the last N results" / "Ensename los ultimos N"

1. Identify the job (latest active or specified)
2. Run `results <job_id> --show N`
3. Display in table format (default) or requested format (json/tsv)

### User asks: "Where are the files?" / "Donde estan los archivos?"

1. Run `scraper_cli.py files`
2. Report output directory, file count, and total size
3. Explain file naming convention

### User asks: "Export results as JSON" / "Dame los datos en JSON"

1. Run `results <job_id> --show N --format json`
2. Provide the JSON output
3. Mention the full CSV path for complete data

## Important Notes

### Cookie Expiration
- Cookies expire after ~7 days of inactivity
- Symptom: scraping returns 0 results
- Solution: run `--setup` again

### Rate Limiting
- Google may throttle if too aggressive
- Using 1 worker is safest
- If results drop to 0 mid-job, pause and wait 1 hour

### Data Quality Tiers
1. **Corporate emails** (marked with star) - Match website domain (highest quality)
2. **Raw emails** - Found on website but generic domains (gmail, hotmail, etc.)
3. **No email** - Business has website but no email found

### Checkpoint System
- Progress saves every 10 businesses
- Safe to interrupt (Ctrl+C, pause, kill) anytime
- Resume picks up exactly where it stopped
- No duplicate processing on resume

## File Structure

When installed, the scraper creates:

```
$SCRAPER_PATH/
├── scrape_maps_interactive.py  # Main scraper script
├── scrape_manager.py           # TUI manager
├── job_manager.py              # Job persistence
├── skill/
│   ├── SKILL.md               # This file
│   └── scripts/
│       ├── scraper_status.py  # Quick status
│       └── scraper_cli.py     # Full CLI
├── scrappings/                 # Default output directory
│   ├── results_*.csv          # Results with emails
│   └── no_emails_*.csv        # Results without emails
├── checkpoints/                # Resume data
├── browser_data/               # Cookies
│   └── google_maps_state.json # Session cookies
└── cache/
    └── jobs/                   # Job registry
```

## Error Handling

| Error | Cause | Solution |
|-------|-------|----------|
| "No cookies found" | First run or cookies deleted | Run `--setup` |
| "Cookies expired" | > 7 days since setup | Run `--setup` again |
| "0 results" | Rate limited or cookies expired | Wait 1h or run `--setup` |
| "Process not responding" | Browser hung | `kill` then `resume` |
| "Port already in use" | Previous instance not cleaned | Kill orphan processes |
| "Output directory does not exist" | Custom dir not created | Create the directory first |

## Resources

### scripts/
- `scraper_status.py` - Lightweight status check, primary monitoring tool
- `scraper_cli.py` - Full CLI for start/stop/pause/resume/results/files

Both scripts auto-detect SCRAPER_PATH from environment or use relative paths.

### Environment Variables
| Variable | Purpose | Default |
|----------|---------|---------|
| `SCRAPER_PATH` | Installation directory | Auto-detected |
| `SCRAPER_OUTPUT_DIR` | Custom output directory | `$SCRAPER_PATH/scrappings` |
| `GEONAMES_USERNAME` | GeoNames API username (optional) | `gorkota` |
