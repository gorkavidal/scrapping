#!/usr/bin/env python3
"""
Scrape Manager - Interfaz interactiva tipo htop para gestionar instancias del scraper.

Funcionalidades:
- Ver instancias activas con métricas en tiempo real
- Pausar/Resumir/Parar instancias
- Modificar número de workers
- Ver histórico de jobs
- Revivir jobs interrumpidos
- Visualizar archivos de resultados
"""

import curses
import json
import os
import signal
import subprocess
import sys
import threading
import time
import csv
import asyncio
import aiohttp
from datetime import datetime
from typing import List, Optional
from collections import deque

from job_manager import (
    JobManager, Job, Instance, JobStatus, JobStats,
    ControlCommand, format_duration, calculate_eta
)


# ---- GeoNames API functions (simplified from scrape_maps_interactive.py) ----

async def fetch_geonames_api(endpoint: str, params: dict) -> dict:
    """Función genérica para llamar a la API de GeoNames."""
    base_url = "http://api.geonames.org/"
    url = f"{base_url}{endpoint}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=30) as response:
                response.raise_for_status()
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' in content_type:
                    data = await response.json()
                else:
                    return {"error": "Formato no JSON"}
                if isinstance(data, dict) and 'status' in data:
                    return {"error": data['status']['message']}
                return data
        except Exception as e:
            return {"error": str(e)}


async def get_regions_for_country(country_code: str, geonames_username: str) -> list:
    """Obtiene las divisiones ADM1 (regiones/estados) para un país."""
    params = {
        'country': country_code.upper(),
        'featureCode': 'ADM1',
        'username': geonames_username,
        'style': 'FULL',
        'type': 'JSON'
    }
    data = await fetch_geonames_api('searchJSON', params)
    regions = []
    if data and 'geonames' in data:
        regions = sorted([
            {
                'name': region.get('adminName1') or region.get('name'),
                'code': region.get('adminCode1'),
                'geonameId': region.get('geonameId')
            }
            for region in data['geonames']
            if (region.get('adminName1') or region.get('name')) and region.get('adminCode1')
        ], key=lambda x: x['name'])
    return regions


async def get_subdivisions_for_region(country_code: str, adm1_code: str, geonames_username: str) -> list:
    """Obtiene las divisiones ADM2 (provincias/condados) para una región ADM1."""
    if not adm1_code:
        return []
    params = {
        'country': country_code.upper(),
        'adminCode1': adm1_code,
        'featureCode': 'ADM2',
        'username': geonames_username,
        'style': 'FULL',
        'type': 'JSON'
    }
    data = await fetch_geonames_api('searchJSON', params)
    subdivisions = []
    if data and 'geonames' in data:
        subdivisions = sorted([
            {
                'name': sub.get('adminName2') or sub.get('name'),
                'code': sub.get('adminCode2'),
                'geonameId': sub.get('geonameId')
            }
            for sub in data['geonames']
            if (sub.get('adminName2') or sub.get('name')) and sub.get('adminCode2')
        ], key=lambda x: x['name'])
    return subdivisions


class ScrapeManager:
    """Interfaz curses para gestionar instancias del scraper.

    Optimizaciones v2:
    - Refresh reducido a 500ms para menos I/O
    - Cache de conteo de líneas CSV para evitar lecturas repetidas
    """

    # Configuración de refresh (en ms)
    REFRESH_TIMEOUT_MS = 500  # Timeout de getch (antes 100ms)
    SLEEP_BETWEEN_FRAMES = 0.0  # Sin sleep adicional (antes 0.05s)

    # Cache para conteo de líneas CSV
    CSV_LINE_CACHE_TTL = 5.0  # Segundos de validez del cache

    def __init__(self, stdscr):
        self.stdscr = stdscr
        self.job_manager = JobManager()
        self.selected_index = 0
        self.view_mode = "active"  # "active", "history", "files"
        self.running = True
        self.message = ""
        self.message_time = 0
        self.message_duration = 3.0

        # Track pending commands for visual feedback
        self.pending_commands = {}  # pid -> (command, timestamp)

        # File viewer state
        self.file_scroll_offset = 0
        self.current_file_path = None
        self.current_file_lines = []
        self.csv_data = []  # Parsed CSV data for table view
        self.show_all_emails = False  # Toggle: False=corporativos, True=todos

        # Cache para conteo de líneas CSV: {filepath: (mtime, count, cached_at)}
        self._csv_line_cache = {}

        # Curses setup
        curses.curs_set(0)  # Hide cursor
        curses.start_color()
        curses.use_default_colors()

        # Define color pairs
        curses.init_pair(1, curses.COLOR_GREEN, -1)   # Running/Success
        curses.init_pair(2, curses.COLOR_YELLOW, -1)  # Paused/Pending
        curses.init_pair(3, curses.COLOR_RED, -1)     # Stopped/Failed
        curses.init_pair(4, curses.COLOR_CYAN, -1)    # Headers
        curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLUE)  # Selected
        curses.init_pair(6, curses.COLOR_MAGENTA, -1)  # Stopping
        curses.init_pair(7, curses.COLOR_RED, curses.COLOR_YELLOW)  # Warning
        curses.init_pair(8, curses.COLOR_WHITE, -1)   # Normal text

        self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)  # Non-blocking getch optimizado

    def get_status_color(self, status: str) -> int:
        """Returns the color pair for a given status."""
        status_colors = {
            JobStatus.RUNNING: 1,
            JobStatus.PAUSED: 2,
            JobStatus.STOPPING: 6,
            JobStatus.COMPLETED: 1,
            JobStatus.INTERRUPTED: 3,
            JobStatus.FAILED: 3,
            JobStatus.PENDING: 2,
        }
        return curses.color_pair(status_colors.get(status, 0))

    def show_message(self, msg: str, duration: float = 3.0):
        """Shows a temporary message at the bottom."""
        self.message = msg
        self.message_time = time.time()
        self.message_duration = duration

    def get_display_status(self, instance: Instance) -> str:
        """Gets display status including pending commands.

        Pending commands persist until the actual status changes to the expected state.
        """
        pid = instance.pid
        if pid in self.pending_commands:
            cmd, ts, expected_status = self.pending_commands[pid]
            # Check if status has changed to expected state
            status_reached = False
            if cmd == "PAUSING" and instance.status == JobStatus.PAUSED:
                status_reached = True
            elif cmd == "RESUMING" and instance.status == JobStatus.RUNNING:
                status_reached = True
            elif cmd == "STOPPING" and instance.status in [JobStatus.STOPPING, JobStatus.COMPLETED, JobStatus.INTERRUPTED]:
                status_reached = True
            elif cmd.startswith("W=") and time.time() - ts > 5:
                # Worker change - clear after 5 seconds
                status_reached = True

            if status_reached:
                del self.pending_commands[pid]
            else:
                return f"{cmd}..."
        return instance.status[:12]

    def format_population(self, pop: int) -> str:
        """Formats population number with K/M suffix."""
        if pop >= 1_000_000:
            return f"{pop/1_000_000:.1f}M"
        elif pop >= 1_000:
            return f"{pop/1_000:.0f}K"
        return str(pop)

    def draw_header(self):
        """Draws the header bar."""
        height, width = self.stdscr.getmaxyx()

        modes = {
            "active": "[1] Activas",
            "history": "[2] Histórico",
            "files": "[3] Archivos"
        }
        mode_str = modes.get(self.view_mode, "")

        title = " SCRAPE MANAGER "
        header = f"{title} - {mode_str}"
        header = header.center(width - 1)

        self.stdscr.attron(curses.color_pair(4) | curses.A_BOLD)
        self.stdscr.addstr(0, 0, header[:width-1])
        self.stdscr.attroff(curses.color_pair(4) | curses.A_BOLD)

    def draw_help_bar(self):
        """Draws the help bar at the bottom."""
        height, width = self.stdscr.getmaxyx()

        if self.view_mode == "active":
            help_text = " [N]uevo [P]ausar [R]esumir [S]top [K]ill [W]orkers [T]Retry [F]iles [1]Act [2]Hist [Q]uit "
        elif self.view_mode == "history":
            help_text = " [N]uevo [Enter]Revivir [D]eliminar [F]iles [1]Act [2]Hist [Q]uit "
        else:  # files
            email_mode = "Todos" if self.show_all_emails else "Corp"
            help_text = f" [↑↓]Scroll [E]mails:{email_mode} [N]uevo [1]Act [2]Hist [Q]uit "

        help_text = help_text.ljust(width - 1)[:width - 1]

        self.stdscr.attron(curses.A_REVERSE)
        try:
            self.stdscr.addstr(height - 1, 0, help_text)
        except curses.error:
            pass
        self.stdscr.attroff(curses.A_REVERSE)

        # Show message if recent
        if self.message and time.time() - self.message_time < self.message_duration:
            msg_line = height - 2
            self.stdscr.addstr(msg_line, 0, " " * (width - 1))
            if "KILL" in self.message or "Error" in self.message:
                self.stdscr.attron(curses.color_pair(3) | curses.A_BOLD)
            elif "enviado" in self.message or "Enviando" in self.message:
                self.stdscr.attron(curses.color_pair(2) | curses.A_BOLD)
            else:
                self.stdscr.attron(curses.color_pair(1) | curses.A_BOLD)
            self.stdscr.addstr(msg_line, 2, self.message[:width - 4])
            self.stdscr.attroff(curses.color_pair(3) | curses.color_pair(2) | curses.color_pair(1) | curses.A_BOLD)

    def draw_active_instances(self):
        """Draws the active instances view."""
        height, width = self.stdscr.getmaxyx()

        # Clean up dead instances first
        self.job_manager.cleanup_dead_instances()
        instances = self.job_manager.list_instances()

        # Column header - with workers column
        header_y = 2
        col_format = "{:<8} {:<10} {:>3} {:>6}/{:<6} {:>8} {:>10} {:<22}"
        header = col_format.format(
            "PID", "Estado", "W", "Cities", "Total", "Emails", "Runtime", "Ciudad Actual"
        )

        self.stdscr.attron(curses.A_BOLD)
        self.stdscr.addstr(header_y, 0, header[:width - 1])
        self.stdscr.attroff(curses.A_BOLD)
        self.stdscr.addstr(header_y + 1, 0, "-" * min(len(header), width - 1))

        if not instances:
            self.stdscr.addstr(header_y + 3, 2, "No hay instancias activas.")
            self.stdscr.addstr(header_y + 5, 2, "Lanza un scraping con:")
            self.stdscr.addstr(header_y + 6, 4, "python scrape_maps_interactive.py --continue-run")
            return

        # Ensure selected_index is valid
        if self.selected_index >= len(instances):
            self.selected_index = len(instances) - 1
        if self.selected_index < 0:
            self.selected_index = 0

        # Draw instances (compact view)
        for i, instance in enumerate(instances):
            y = header_y + 3 + i
            if y >= height - 12:  # Leave room for details
                break

            stats = instance.stats
            config = instance.config
            display_status = self.get_display_status(instance)
            runtime = format_duration(stats.runtime_seconds)
            current_city = stats.current_city[:20] if stats.current_city else "-"
            workers = config.get('workers', 1)

            line = col_format.format(
                instance.pid,
                display_status,
                workers,
                stats.cities_processed,
                stats.cities_total,
                stats.results_with_email,
                runtime,
                current_city
            )

            if i == self.selected_index:
                self.stdscr.attron(curses.color_pair(5))
            elif instance.pid in self.pending_commands:
                self.stdscr.attron(curses.color_pair(2) | curses.A_BOLD)
            else:
                self.stdscr.attron(self.get_status_color(instance.status))

            try:
                self.stdscr.addstr(y, 0, line[:width - 1].ljust(width - 1))
            except curses.error:
                pass

            if i == self.selected_index:
                self.stdscr.attroff(curses.color_pair(5))
            elif instance.pid in self.pending_commands:
                self.stdscr.attroff(curses.color_pair(2) | curses.A_BOLD)
            else:
                self.stdscr.attroff(self.get_status_color(instance.status))

        # Draw detailed stats for selected instance
        if instances and self.selected_index < len(instances):
            detail_y = header_y + 3 + min(len(instances), height - 15) + 1
            self.draw_instance_details_extended(instances[self.selected_index], detail_y)

    def draw_instance_details_extended(self, instance: Instance, start_y: int):
        """Draws extended detailed info for the selected instance."""
        height, width = self.stdscr.getmaxyx()
        num_workers = instance.config.get('workers', 1)
        # Necesitamos más espacio si hay múltiples workers (2 líneas por worker)
        min_space_needed = 8 + (num_workers * 2)
        if start_y >= height - min_space_needed:
            return

        stats = instance.stats
        config = instance.config
        job = self.job_manager.load_job(instance.job_id)

        self.stdscr.addstr(start_y, 0, "=" * (width - 1))

        # Title (includes run_id if available)
        run_id = config.get('run_id', '')
        run_id_part = f" | Run: {run_id}" if run_id else ""
        self.stdscr.attron(curses.color_pair(4) | curses.A_BOLD)
        title = f" PID {instance.pid} | {config.get('query', 'N/A')} en {config.get('country_code', 'N/A')}{run_id_part} "
        self.stdscr.addstr(start_y + 1, 2, title[:width - 4])
        self.stdscr.attroff(curses.color_pair(4) | curses.A_BOLD)

        # Progress section
        y = start_y + 3
        cities_remaining = stats.cities_total - stats.cities_processed
        progress_pct = (stats.cities_processed / stats.cities_total * 100) if stats.cities_total > 0 else 0

        # Ciudades activas con progreso granular (soporta múltiples workers)
        active_cities = getattr(stats, 'active_cities', {}) or {}
        num_workers = config.get('workers', 1)

        if active_cities:
            # Mostrar todas las ciudades activas (múltiples workers)
            self.stdscr.attron(curses.A_BOLD)
            self.stdscr.addstr(y, 4, f"Ciudades en proceso ({len(active_cities)} de {num_workers} workers):")
            self.stdscr.attroff(curses.A_BOLD)
            y += 1

            for city_name, city_data in active_cities.items():
                if y >= height - 10:  # Evitar overflow
                    break
                worker_id = city_data.get('worker_id', '?')
                population = city_data.get('population', 0)
                biz_total = city_data.get('businesses_total', 0)
                biz_processed = city_data.get('businesses_processed', 0)
                status = city_data.get('status', 'searching')

                # Info de la ciudad
                city_info = f"  W{worker_id}: {city_name}"
                if population > 0:
                    city_info += f" ({self.format_population(population)} hab)"
                self.stdscr.addstr(y, 4, city_info[:width - 6])
                y += 1

                # Mostrar estado o barra de progreso
                if status == 'searching':
                    # Animación de búsqueda inicial
                    search_dots = "." * ((int(time.time()) % 3) + 1)
                    search_line = f"      └─ 🔍 Conectando a Maps{search_dots}"
                    self.stdscr.attron(curses.color_pair(4))  # Cyan
                    self.stdscr.addstr(y, 4, search_line[:width - 6])
                    self.stdscr.attroff(curses.color_pair(4))
                    y += 1
                elif status == 'scrolling':
                    # Scroll en progreso - mostrar resultados encontrados
                    scroll_results = city_data.get('scroll_results', 0)
                    search_dots = "." * ((int(time.time()) % 3) + 1)
                    search_line = f"      └─ 🔍 Scroll en Maps{search_dots} ({scroll_results} encontrados)"
                    self.stdscr.attron(curses.color_pair(4))  # Cyan
                    self.stdscr.addstr(y, 4, search_line[:width - 6])
                    self.stdscr.attroff(curses.color_pair(4))
                    y += 1
                elif status == 'opening_cards':
                    # Abriendo fichas individuales
                    cards_total = city_data.get('cards_total', 0)
                    cards_opened = city_data.get('cards_opened', 0)
                    scroll_results = city_data.get('scroll_results', 0)
                    search_dots = "." * ((int(time.time()) % 3) + 1)
                    search_line = f"      └─ 📋 Abriendo fichas{search_dots} {cards_opened}/{cards_total} (de {scroll_results} total)"
                    self.stdscr.attron(curses.color_pair(6))  # Magenta
                    self.stdscr.addstr(y, 4, search_line[:width - 6])
                    self.stdscr.attroff(curses.color_pair(6))
                    y += 1
                elif biz_total > 0:
                    # Barra de progreso de negocios (extracting)
                    biz_pct = (biz_processed / biz_total * 100) if biz_total > 0 else 0
                    mini_bar_width = 20
                    mini_filled = int(mini_bar_width * biz_pct / 100)
                    mini_bar = "▓" * mini_filled + "░" * (mini_bar_width - mini_filled)
                    biz_line = f"      └─ [{mini_bar}] {biz_processed}/{biz_total} ({biz_pct:.0f}%)"
                    self.stdscr.attron(curses.color_pair(2))  # Amarillo
                    self.stdscr.addstr(y, 4, biz_line[:width - 6])
                    self.stdscr.attroff(curses.color_pair(2))
                    y += 1
        elif stats.current_city:
            # Fallback: mostrar ciudad única (1 worker o legacy)
            city_info = f"Ciudad actual: {stats.current_city}"
            if stats.current_city_population > 0:
                city_info += f" ({self.format_population(stats.current_city_population)} hab)"
            if stats.current_city_index > 0:
                city_info += f" [{stats.current_city_index}/{stats.cities_total}]"
            self.stdscr.addstr(y, 4, city_info[:width - 6])
            y += 1

            # Progreso granular dentro de la ciudad (negocios procesados)
            if stats.current_city_businesses_total > 0:
                biz_processed = stats.current_city_businesses_processed
                biz_total = stats.current_city_businesses_total
                biz_pct = (biz_processed / biz_total * 100) if biz_total > 0 else 0
                mini_bar_width = 20
                mini_filled = int(mini_bar_width * biz_pct / 100)
                mini_bar = "▓" * mini_filled + "░" * (mini_bar_width - mini_filled)
                biz_line = f"  └─ Negocios: [{mini_bar}] {biz_processed}/{biz_total} ({biz_pct:.0f}%)"
                self.stdscr.attron(curses.color_pair(2))  # Amarillo
                self.stdscr.addstr(y, 4, biz_line[:width - 6])
                self.stdscr.attroff(curses.color_pair(2))
                y += 1
            else:
                # Todavía buscando en Maps
                search_dots = "." * ((int(time.time()) % 3) + 1)
                search_line = f"  └─ 🔍 Buscando en Maps{search_dots}"
                self.stdscr.attron(curses.color_pair(4))  # Cyan
                self.stdscr.addstr(y, 4, search_line[:width - 6])
                self.stdscr.attroff(curses.color_pair(4))
                y += 1

        # Progress bar
        bar_width = min(40, width - 20)
        filled = int(bar_width * progress_pct / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        progress_line = f"Progreso: [{bar}] {progress_pct:.1f}%"
        self.stdscr.addstr(y, 4, progress_line[:width - 6])
        y += 1

        # Cities stats
        cities_line = f"Ciudades: {stats.cities_processed} completadas, {cities_remaining} restantes de {stats.cities_total}"
        self.stdscr.addstr(y, 4, cities_line[:width - 6])
        y += 1

        # Time stats
        runtime = format_duration(stats.runtime_seconds)
        avg_time = format_duration(stats.avg_city_duration) if stats.avg_city_duration > 0 else "-"
        last_time = format_duration(stats.last_city_duration) if stats.last_city_duration > 0 else "-"
        eta = calculate_eta(stats)

        time_line = f"Tiempo: {runtime} total | {avg_time}/ciudad (media) | {last_time} (última) | ETA: {eta}"
        self.stdscr.addstr(y, 4, time_line[:width - 6])
        y += 1

        # Results stats
        self.stdscr.attron(curses.color_pair(1))
        results_line = f"Resultados: {stats.results_total} total | {stats.results_with_email} con email | {stats.results_with_corporate_email} corporativos | {stats.results_with_web} solo web"
        self.stdscr.addstr(y, 4, results_line[:width - 6])
        self.stdscr.attroff(curses.color_pair(1))
        y += 1

        # Files
        if job:
            y += 1
            self.stdscr.attron(curses.A_BOLD)
            self.stdscr.addstr(y, 4, "Archivos:")
            self.stdscr.attroff(curses.A_BOLD)
            y += 1

            if job.csv_with_emails:
                file_size = self.get_file_size(job.csv_with_emails)
                line_count = self.count_csv_lines(job.csv_with_emails)
                self.stdscr.addstr(y, 6, f"Con emails: {os.path.basename(job.csv_with_emails)} ({line_count} registros, {file_size})")
                y += 1
            if job.csv_no_emails:
                file_size = self.get_file_size(job.csv_no_emails)
                line_count = self.count_csv_lines(job.csv_no_emails)
                self.stdscr.addstr(y, 6, f"Sin emails: {os.path.basename(job.csv_no_emails)} ({line_count} registros, {file_size})")

    def get_file_size(self, filepath: str) -> str:
        """Returns formatted file size."""
        try:
            size = os.path.getsize(filepath)
            if size >= 1_000_000:
                return f"{size/1_000_000:.1f}MB"
            elif size >= 1_000:
                return f"{size/1_000:.1f}KB"
            return f"{size}B"
        except:
            return "-"

    def count_csv_lines(self, filepath: str) -> int:
        """Counts lines in a CSV file (excluding header).

        Usa cache con TTL para evitar lecturas repetidas.
        El cache se invalida si el archivo ha cambiado (mtime).
        """
        now = time.time()

        # Verificar cache
        if filepath in self._csv_line_cache:
            cached_mtime, cached_count, cached_at = self._csv_line_cache[filepath]
            # Verificar TTL y mtime
            if now - cached_at < self.CSV_LINE_CACHE_TTL:
                try:
                    current_mtime = os.path.getmtime(filepath)
                    if current_mtime == cached_mtime:
                        return cached_count
                except OSError:
                    pass

        # Leer de disco
        try:
            mtime = os.path.getmtime(filepath)
            with open(filepath, 'r', encoding='utf-8') as f:
                count = sum(1 for _ in f) - 1  # Exclude header
            # Guardar en cache
            self._csv_line_cache[filepath] = (mtime, count, now)
            return count
        except:
            return 0

    def draw_history(self):
        """Draws the job history view."""
        height, width = self.stdscr.getmaxyx()

        jobs = self.job_manager.list_jobs()

        header_y = 2
        col_format = "{:<28} {:<11} {:>6}/{:<6} {:>8} {:>10} {:<18}"
        header = col_format.format(
            "Job ID", "Estado", "Cities", "Total", "Emails", "Runtime", "Finalizado"
        )

        self.stdscr.attron(curses.A_BOLD)
        self.stdscr.addstr(header_y, 0, header[:width - 1])
        self.stdscr.attroff(curses.A_BOLD)
        self.stdscr.addstr(header_y + 1, 0, "-" * min(len(header), width - 1))

        if not jobs:
            self.stdscr.addstr(header_y + 3, 2, "No hay jobs en el histórico.")
            return

        if self.selected_index >= len(jobs):
            self.selected_index = len(jobs) - 1
        if self.selected_index < 0:
            self.selected_index = 0

        max_visible = height - header_y - 6
        start_idx = max(0, self.selected_index - max_visible + 1)

        for i, job in enumerate(jobs[start_idx:start_idx + max_visible]):
            actual_idx = start_idx + i
            y = header_y + 3 + i
            if y >= height - 3:
                break

            stats = job.stats
            runtime = format_duration(stats.runtime_seconds) if stats.runtime_seconds > 0 else "-"
            finished = job.finished_at[5:16] if job.finished_at else "-"

            line = col_format.format(
                job.job_id[:26],
                job.status[:10],
                stats.cities_processed,
                stats.cities_total,
                stats.results_with_email,
                runtime,
                finished
            )

            if actual_idx == self.selected_index:
                self.stdscr.attron(curses.color_pair(5))
            else:
                self.stdscr.attron(self.get_status_color(job.status))

            try:
                self.stdscr.addstr(y, 0, line[:width - 1].ljust(width - 1))
            except curses.error:
                pass

            if actual_idx == self.selected_index:
                self.stdscr.attroff(curses.color_pair(5))
            else:
                self.stdscr.attroff(self.get_status_color(job.status))

    def draw_files_view(self):
        """Draws the file viewer with formatted CSV table."""
        height, width = self.stdscr.getmaxyx()

        header_y = 2
        self.stdscr.attron(curses.A_BOLD)
        if self.current_file_path:
            self.stdscr.addstr(header_y, 2, f"Archivo: {os.path.basename(self.current_file_path)}")
        else:
            self.stdscr.addstr(header_y, 2, "Selecciona una instancia y pulsa [F] para ver archivos")
        self.stdscr.attroff(curses.A_BOLD)
        self.stdscr.addstr(header_y + 1, 0, "-" * (width - 1))

        if not self.csv_data:
            self.stdscr.addstr(header_y + 3, 2, "No hay archivo cargado.")
            return

        # Calculate column widths based on terminal width
        # Columns: #, Ciudad, Emails, Nombre
        total_records = len(self.csv_data)
        num_width = len(str(total_records)) + 1  # Width for row number
        city_width = 18
        email_width = max(35, (width - num_width - city_width - 6) // 2)  # Give emails more space
        name_width = width - num_width - city_width - email_width - 6

        # Draw table header
        table_header_y = header_y + 3
        email_header = "Email(s) [Todos]" if self.show_all_emails else "Email(s) [Corp]"
        self.stdscr.attron(curses.A_BOLD | curses.A_REVERSE)
        header_line = f" {'#':>{num_width}} {'Ciudad':<{city_width}} {email_header:<{email_width}} {'Nombre':<{name_width}}"
        try:
            self.stdscr.addstr(table_header_y, 0, header_line[:width - 1].ljust(width - 1))
        except curses.error:
            pass
        self.stdscr.attroff(curses.A_BOLD | curses.A_REVERSE)

        # Show data rows
        visible_lines = height - table_header_y - 4
        total_lines = len(self.csv_data)

        # Ensure scroll offset is valid
        max_offset = max(0, total_lines - visible_lines)
        self.file_scroll_offset = min(self.file_scroll_offset, max_offset)
        self.file_scroll_offset = max(0, self.file_scroll_offset)

        # Show scroll position
        scroll_info = f"Registros {self.file_scroll_offset + 1}-{min(self.file_scroll_offset + visible_lines, total_lines)} de {total_lines}"
        self.stdscr.addstr(header_y, width - len(scroll_info) - 2, scroll_info)

        # Draw data rows
        for i in range(visible_lines):
            row_idx = self.file_scroll_offset + i
            y = table_header_y + 1 + i

            if row_idx >= total_lines:
                break

            row = self.csv_data[row_idx]
            row_num = row.get('_row_num', row_idx + 1)
            city = row.get('Localidad', '-')[:city_width]
            # Choose email field based on toggle - NO mixing
            if self.show_all_emails:
                # Show all emails (Email_Raw)
                emails = row.get('Email_Raw', '')
            else:
                # Show only corporate emails (Email_Filtered)
                emails = row.get('Email_Filtered', '')
            # Format for display
            if emails and emails.strip():
                emails = emails.replace(';', ', ')[:email_width]
            else:
                emails = '-'
            name = row.get('Name', '-')[:name_width]

            line = f" {row_num:>{num_width}} {city:<{city_width}} {emails:<{email_width}} {name:<{name_width}}"

            # Alternate row colors for readability
            if i % 2 == 0:
                self.stdscr.attron(curses.color_pair(8))
            else:
                self.stdscr.attron(curses.A_DIM)

            try:
                self.stdscr.addstr(y, 0, line[:width - 1])
            except curses.error:
                pass

            if i % 2 == 0:
                self.stdscr.attroff(curses.color_pair(8))
            else:
                self.stdscr.attroff(curses.A_DIM)

    def load_file_tail(self, filepath: str, max_lines: int = 100):
        """Loads the last N rows of a CSV file as structured data."""
        try:
            self.csv_data = []
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                all_rows = list(reader)

                # Get last N rows with row numbers
                start_idx = max(0, len(all_rows) - max_lines)
                for i, row in enumerate(all_rows[start_idx:], start=start_idx + 1):
                    row['_row_num'] = i
                    self.csv_data.append(row)

            self.current_file_path = filepath
            self.current_file_lines = []  # Clear old format
            self.file_scroll_offset = max(0, len(self.csv_data) - 15)  # Start near end
            self.view_mode = "files"
            self.show_message(f"Cargados {len(self.csv_data)} registros de {os.path.basename(filepath)}")
        except Exception as e:
            self.show_message(f"Error leyendo archivo: {e}")

    def handle_input(self, key: int):
        """Handles keyboard input."""
        if key == ord('q') or key == ord('Q'):
            self.running = False
            return

        # View switching
        if key == ord('1'):
            self.view_mode = "active"
            self.selected_index = 0
            return
        if key == ord('2'):
            self.view_mode = "history"
            self.selected_index = 0
            return
        if key == ord('n') or key == ord('N'):
            self.new_scraping_wizard()
            return

        # File view scrolling and commands
        if self.view_mode == "files":
            if key == curses.KEY_UP or key == ord('k'):
                self.file_scroll_offset = max(0, self.file_scroll_offset - 1)
                return
            if key == curses.KEY_DOWN or key == ord('j'):
                self.file_scroll_offset += 1
                return
            if key == curses.KEY_PPAGE:  # Page Up
                self.file_scroll_offset = max(0, self.file_scroll_offset - 20)
                return
            if key == curses.KEY_NPAGE:  # Page Down
                self.file_scroll_offset += 20
                return
            if key == ord('e') or key == ord('E'):
                # Toggle between corporate emails and all emails
                self.show_all_emails = not self.show_all_emails
                mode = "todos los emails" if self.show_all_emails else "emails corporativos"
                self.show_message(f"Mostrando {mode}")
                return
            return

        # Navigation for other views
        if key == curses.KEY_UP or key == ord('k'):
            self.selected_index = max(0, self.selected_index - 1)
            return
        if key == curses.KEY_DOWN or key == ord('j'):
            self.selected_index += 1
            return

        # Active view commands
        if self.view_mode == "active":
            instances = self.job_manager.list_instances()
            if not instances or self.selected_index >= len(instances):
                return

            selected_instance = instances[self.selected_index]
            pid = selected_instance.pid

            if key == ord('p') or key == ord('P'):
                self.send_command_with_feedback(pid, ControlCommand.PAUSE, "PAUSING")
                return

            if key == ord('r') or key == ord('R'):
                self.send_command_with_feedback(pid, ControlCommand.RESUME, "RESUMING")
                return

            if key == ord('s') or key == ord('S'):
                self.send_command_with_feedback(pid, ControlCommand.STOP, "STOPPING")
                return

            if key == ord('K'):  # Only uppercase K (lowercase k is for navigation)
                self.kill_instance(pid)
                return

            if key == ord('w') or key == ord('W'):
                self.prompt_workers(pid)
                return

            if key == ord('f') or key == ord('F'):
                # Load file for current instance
                job = self.job_manager.load_job(selected_instance.job_id)
                if job and job.csv_with_emails and os.path.exists(job.csv_with_emails):
                    self.load_file_tail(job.csv_with_emails, 200)
                else:
                    self.show_message("No hay archivo de resultados disponible")
                return

            if key == ord('t') or key == ord('T'):
                # Retry ciudad atascada
                self.prompt_retry_city(selected_instance)
                return

        # History view commands
        if self.view_mode == "history":
            jobs = self.job_manager.list_jobs()
            if not jobs or self.selected_index >= len(jobs):
                return

            selected_job = jobs[self.selected_index]

            if key == ord('\n') or key == curses.KEY_ENTER or key == 10:
                if selected_job.status in [JobStatus.INTERRUPTED, JobStatus.FAILED]:
                    self.revive_job(selected_job)
                else:
                    self.show_message("Solo se pueden revivir jobs interrumpidos o fallidos")
                return

            if key == ord('d') or key == ord('D'):
                if self.job_manager.delete_job(selected_job.job_id):
                    self.show_message(f"Job eliminado")
                    if self.selected_index > 0:
                        self.selected_index -= 1
                else:
                    self.show_message(f"Error eliminando job")
                return

            if key == ord('f') or key == ord('F'):
                if selected_job.csv_with_emails and os.path.exists(selected_job.csv_with_emails):
                    self.load_file_tail(selected_job.csv_with_emails, 200)
                else:
                    self.show_message("No hay archivo de resultados disponible")
                return

    def send_command_with_feedback(self, pid: int, command: str, display_name: str):
        """Sends a command and shows visual feedback.

        The pending command persists until the expected status is reached.
        """
        self.show_message(f"Enviando {display_name} a PID {pid}...")
        # Store (display_name, timestamp, expected_status)
        self.pending_commands[pid] = (display_name, time.time(), command)

        if self.job_manager.send_command(pid, command):
            self.show_message(f"{display_name} enviado a PID {pid}. Esperando respuesta...")
        else:
            self.show_message(f"Error enviando {display_name} a PID {pid}")
            if pid in self.pending_commands:
                del self.pending_commands[pid]

    def kill_instance(self, pid: int):
        """Kills an instance immediately with SIGKILL."""
        height, width = self.stdscr.getmaxyx()

        self.stdscr.addstr(height - 3, 2, " " * (width - 4))
        self.stdscr.attron(curses.color_pair(7) | curses.A_BOLD)
        self.stdscr.addstr(height - 3, 2, f"¿KILL PID {pid}? Puede perder la ciudad actual. [s/N]: ")
        self.stdscr.attroff(curses.color_pair(7) | curses.A_BOLD)
        self.stdscr.refresh()

        self.stdscr.timeout(-1)
        try:
            confirm = self.stdscr.getch()
            if confirm == ord('s') or confirm == ord('S'):
                try:
                    # Get job info before killing
                    instance = self.job_manager.load_instance(pid)

                    os.kill(pid, signal.SIGKILL)
                    self.show_message(f"KILL enviado a PID {pid}. Proceso terminado.", 5.0)

                    # Clean up and mark as interrupted
                    if instance:
                        job = self.job_manager.load_job(instance.job_id)
                        if job:
                            job.status = JobStatus.INTERRUPTED
                            job.finished_at = datetime.now().isoformat()
                            self.job_manager.save_job(job)
                    self.job_manager.unregister_instance(pid)

                except ProcessLookupError:
                    self.show_message(f"Proceso {pid} no encontrado (ya terminó)")
                except PermissionError:
                    self.show_message(f"Sin permisos para matar PID {pid}")
                except Exception as e:
                    self.show_message(f"Error: {e}")
            else:
                self.show_message("Kill cancelado")
        finally:
            self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)

    def prompt_retry_city(self, instance: Instance):
        """Permite seleccionar y reiniciar una ciudad atascada."""
        height, width = self.stdscr.getmaxyx()
        pid = instance.pid
        stats = instance.stats
        active_cities = getattr(stats, 'active_cities', {}) or {}

        if not active_cities:
            self.show_message("No hay ciudades en proceso para reiniciar")
            return

        city_names = list(active_cities.keys())

        # Si solo hay una ciudad, preguntar directamente
        if len(city_names) == 1:
            city_to_retry = city_names[0]
            self.stdscr.addstr(height - 3, 2, " " * (width - 4))
            self.stdscr.attron(curses.color_pair(2) | curses.A_BOLD)
            self.stdscr.addstr(height - 3, 2, f"¿Reiniciar '{city_to_retry}'? [s/N]: ")
            self.stdscr.attroff(curses.color_pair(2) | curses.A_BOLD)
            self.stdscr.refresh()

            self.stdscr.timeout(-1)
            try:
                confirm = self.stdscr.getch()
                if confirm == ord('s') or confirm == ord('S'):
                    self.send_retry_city(pid, city_to_retry)
                else:
                    self.show_message("Retry cancelado")
            finally:
                self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)
        else:
            # Múltiples ciudades - mostrar menú
            selected_city_idx = 0
            self.stdscr.timeout(-1)
            try:
                while True:
                    # Dibujar menú de ciudades
                    self.stdscr.addstr(height - 4, 2, " " * (width - 4))
                    self.stdscr.addstr(height - 3, 2, " " * (width - 4))
                    self.stdscr.attron(curses.A_BOLD)
                    self.stdscr.addstr(height - 4, 2, "Selecciona ciudad a reiniciar (↑/↓, Enter=OK, Esc=Cancelar):")
                    self.stdscr.attroff(curses.A_BOLD)

                    # Mostrar ciudades
                    cities_str = ""
                    for i, city in enumerate(city_names):
                        if i == selected_city_idx:
                            cities_str += f"[{city}] "
                        else:
                            cities_str += f" {city}  "
                    self.stdscr.addstr(height - 3, 2, cities_str[:width - 4])
                    self.stdscr.refresh()

                    key = self.stdscr.getch()
                    if key == 27:  # Esc
                        self.show_message("Retry cancelado")
                        break
                    elif key == curses.KEY_LEFT or key == ord('h'):
                        selected_city_idx = max(0, selected_city_idx - 1)
                    elif key == curses.KEY_RIGHT or key == ord('l'):
                        selected_city_idx = min(len(city_names) - 1, selected_city_idx + 1)
                    elif key == ord('\n') or key == curses.KEY_ENTER or key == 10:
                        city_to_retry = city_names[selected_city_idx]
                        self.send_retry_city(pid, city_to_retry)
                        break
            finally:
                self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)

    def send_retry_city(self, pid: int, city_name: str):
        """Envía comando de retry para una ciudad específica."""
        from job_manager import ControlCommand
        self.show_message(f"Enviando RETRY para '{city_name}'...")

        if self.job_manager.send_command(pid, ControlCommand.RETRY_CITY, value=city_name):
            self.show_message(f"RETRY enviado para '{city_name}'. La ciudad se reiniciará.", 5.0)
        else:
            self.show_message(f"Error enviando RETRY para '{city_name}'")

    def prompt_workers(self, pid: int):
        """Prompts for new worker count, updates checkpoint, stops job and relaunches.

        Since workers can't be changed dynamically, this:
        1. Asks for new worker count
        2. Updates the checkpoint file with new workers
        3. Sends STOP command to gracefully stop current job
        4. Relaunches with --resume to pick up new workers
        """
        height, width = self.stdscr.getmaxyx()
        prompt_y = height - 3

        # Get instance and job info
        instance = self.job_manager.load_instance(pid)
        if not instance:
            self.show_message("Instancia no encontrada")
            return

        job = self.job_manager.load_job(instance.job_id)
        if not job:
            self.show_message("Job no encontrado")
            return

        current_workers = instance.config.get('workers', 1)

        self.stdscr.addstr(prompt_y, 0, " " * (width - 1))
        self.stdscr.addstr(prompt_y, 2, f"Workers actuales: {current_workers}. Nuevo número (1-10): ")
        self.stdscr.refresh()

        self.stdscr.timeout(-1)
        curses.echo()
        curses.curs_set(1)

        try:
            self.stdscr.move(prompt_y, 52)
            input_bytes = self.stdscr.getstr(3)
            input_str = input_bytes.decode('utf-8').strip()

            if not input_str:
                self.show_message("Cancelado")
                return

            new_workers = int(input_str)
            if not (1 <= new_workers <= 10):
                self.show_message("Número de workers debe estar entre 1 y 10")
                return

            if new_workers == current_workers:
                self.show_message("El número de workers ya es el solicitado")
                return

            # Find and update the checkpoint file using run_id
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cache_dir = os.path.join(script_dir, "cache")

            # Get run_id from job config
            run_id = job.config.get('run_id')
            if run_id:
                checkpoint_file = os.path.join(cache_dir, f"checkpoint_{run_id}.json")
                if not os.path.exists(checkpoint_file):
                    self.show_message(f"No se encontró checkpoint para run_id {run_id}")
                    return
            else:
                # Fallback: search by query/country for old checkpoints without run_id
                checkpoint_file = None
                if os.path.exists(cache_dir):
                    for filename in os.listdir(cache_dir):
                        if filename.startswith("checkpoint_") and filename.endswith(".json"):
                            filepath = os.path.join(cache_dir, filename)
                            try:
                                with open(filepath, 'r', encoding='utf-8') as f:
                                    data = json.load(f)
                                # Check if this checkpoint matches the job config
                                if data.get('config', {}).get('query') == job.config.get('query') and \
                                   data.get('config', {}).get('country_code') == job.config.get('country_code'):
                                    checkpoint_file = filepath
                                    break
                            except:
                                continue

                if not checkpoint_file:
                    self.show_message("No se encontró el checkpoint para este job")
                    return

            # Update checkpoint with new workers
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)

            checkpoint_data['config']['workers'] = new_workers
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=4)

            script_path = os.path.join(script_dir, "scrape_maps_interactive.py")
            log_file = os.path.join(script_dir, "scrapping_background.log")

            is_paused = instance.status == JobStatus.PAUSED
            start_time = time.time()

            if is_paused:
                # If paused, kill directly - checkpoint is already saved
                try:
                    os.kill(pid, signal.SIGKILL)
                    time.sleep(0.5)  # Brief pause for kill to take effect
                except OSError:
                    pass
                kill_method = "kill (pausado)"
            else:
                # If running, send STOP and wait for graceful termination
                self.job_manager.send_command(pid, ControlCommand.STOP)

                # Wait for process to stop with interactive feedback
                self.stdscr.timeout(500)  # Check every 500ms
                curses.noecho()
                curses.curs_set(0)

                force_killed = False

                while True:
                    # Check if process has finished by verifying instance unregistration
                    # The scraper calls unregister_instance() when it stops gracefully
                    instance = self.job_manager.load_instance(pid)
                    if instance is None:
                        # Process finished and unregistered itself
                        break

                    # Update display
                    elapsed = int(time.time() - start_time)
                    self.stdscr.addstr(prompt_y, 0, " " * (width - 1))
                    self.stdscr.addstr(prompt_y, 2,
                        f"Esperando fin... {elapsed}s  [F]=Forzar kill  [Esc]=Cancelar")
                    self.stdscr.refresh()

                    # Check for user input
                    key = self.stdscr.getch()
                    if key == ord('f') or key == ord('F'):
                        # Force kill
                        try:
                            os.kill(pid, signal.SIGKILL)
                            force_killed = True
                            self.stdscr.addstr(prompt_y, 0, " " * (width - 1))
                            self.stdscr.addstr(prompt_y, 2, "Forzando terminación...")
                            self.stdscr.refresh()
                            time.sleep(0.5)
                        except OSError:
                            pass
                        break
                    elif key == 27:  # Escape
                        self.show_message("Cancelado - el proceso sigue ejecutándose")
                        return

                kill_method = "forzado" if force_killed else "graceful"

            # Process is dead, clean up and relaunch
            elapsed = int(time.time() - start_time)

            try:
                # Clean up old instance and job
                self.job_manager.unregister_instance(pid)
                self.job_manager.delete_job(job.job_id)

                with open(log_file, 'a') as lf:
                    lf.write(f"\n{'='*60}\n")
                    lf.write(f"Relanzando con {new_workers} workers (esperó {elapsed}s, {kill_method})\n")
                    lf.write(f"Run ID: {run_id}\n")
                    lf.write(f"Timestamp: {datetime.now().isoformat()}\n")
                    lf.write(f"{'='*60}\n")
                    # Use --run-id to resume the specific run
                    cmd = [sys.executable, script_path, '--resume', '--batch']
                    if run_id:
                        cmd.extend(['--run-id', run_id])
                    subprocess.Popen(
                        cmd,
                        stdout=lf, stderr=subprocess.STDOUT,
                        start_new_session=True
                    )

                self.show_message(f"Relanzado con {new_workers} workers ({kill_method}, {elapsed}s)", 5.0)
            except Exception as e:
                self.show_message(f"Error relanzando: {e}")

        except ValueError:
            self.show_message("Entrada inválida - debe ser un número")
        except Exception as e:
            self.show_message(f"Error: {e}")
        finally:
            curses.noecho()
            curses.curs_set(0)
            self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)

    def revive_job(self, job: Job):
        """Revives an interrupted job by launching a new process.

        The old job entry is deleted from history to avoid duplicates.
        - If checkpoint exists: uses --resume to continue from where it left off
        - If no checkpoint: uses --batch with saved config to start fresh
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(script_dir, "scrape_maps_interactive.py")
        log_file = os.path.join(script_dir, "scrapping_background.log")

        # Check if checkpoint exists
        has_checkpoint = job.checkpoint_file and os.path.exists(job.checkpoint_file)
        run_id = job.config.get('run_id')

        if has_checkpoint:
            # Resume from checkpoint
            cmd_parts = [
                sys.executable, script_path,
                '--resume',
                '--batch'
            ]
            if run_id:
                cmd_parts.extend(['--run-id', run_id])
            mode_msg = "retomando desde checkpoint"
        else:
            # Start fresh with saved config
            config = job.config
            cmd_parts = [
                sys.executable, script_path,
                '--batch',
                '--country_code', config.get('country_code', 'US'),
                '--query', config.get('query', ''),
                '--min_population', str(config.get('min_population', 50000)),
                '--strategy', config.get('strategy', 'simple'),
                '--max_results', str(config.get('max_results', 300)),
                '--workers', str(config.get('workers', 1)),
            ]
            if config.get('max_population'):
                cmd_parts.extend(['--max_population', str(config['max_population'])])
            if config.get('region_code'):
                cmd_parts.extend(['--region_code', config['region_code']])
            if config.get('adm2_code'):
                cmd_parts.extend(['--adm2_code', config['adm2_code']])
            mode_msg = "comenzando de cero (sin checkpoint)"

        try:
            with open(log_file, 'a') as lf:
                lf.write(f"\n{'='*60}\n")
                lf.write(f"Reviviendo job: {job.job_id} ({mode_msg})\n")
                if run_id and has_checkpoint:
                    lf.write(f"Run ID: {run_id}\n")
                lf.write(f"Timestamp: {datetime.now().isoformat()}\n")
                lf.write(f"Command: {' '.join(cmd_parts)}\n")
                lf.write(f"{'='*60}\n")
                proc = subprocess.Popen(
                    cmd_parts,
                    stdout=lf, stderr=subprocess.STDOUT,
                    start_new_session=True
                )

            # Delete old job from history to avoid duplicates
            self.job_manager.delete_job(job.job_id)
            if self.selected_index > 0:
                self.selected_index -= 1

            self.show_message(f"Job revivido ({mode_msg}) PID {proc.pid}", 5.0)
        except Exception as e:
            self.show_message(f"Error reviviendo job: {e}")

    def check_cookies_valid(self) -> tuple[bool, str]:
        """Checks if Google Maps cookies exist and are recent (less than 7 days old).

        Returns (is_valid, message).
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookies_file = os.path.join(script_dir, "browser_data", "google_maps_state.json")

        if not os.path.exists(cookies_file):
            return False, "No hay cookies guardadas"

        try:
            # Check file age
            file_mtime = os.path.getmtime(cookies_file)
            age_days = (time.time() - file_mtime) / (24 * 3600)

            if age_days > 7:
                return False, f"Cookies antiguas ({int(age_days)} días)"

            # Check file is valid JSON and has content
            with open(cookies_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not data:
                    return False, "Archivo de cookies vacío"

            return True, f"Cookies válidas ({int(age_days)} días)"
        except Exception as e:
            return False, f"Error leyendo cookies: {e}"

    def run_setup_mode(self):
        """Runs the scraper in --setup mode to get fresh cookies.

        This opens a visible browser for the user to complete verification.
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(script_dir, "scrape_maps_interactive.py")

        # Temporarily exit curses to run setup interactively
        curses.endwin()

        print("\n" + "=" * 60)
        print("MODO SETUP - Configuración de cookies")
        print("=" * 60)
        print("\nSe abrirá un navegador visible para verificar Google Maps.")
        print("Completa cualquier verificación/captcha que aparezca.")
        print("Cuando termines, cierra el navegador o pulsa Ctrl+C.\n")

        try:
            subprocess.run([sys.executable, script_path, '--setup'], check=False)
        except KeyboardInterrupt:
            pass

        print("\nVolviendo al gestor...")
        time.sleep(1)

        # Reinitialize curses
        self.stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(0)
        curses.start_color()
        curses.use_default_colors()
        self.stdscr.keypad(True)
        self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)

    def new_scraping_wizard(self):
        """Interactive wizard to create and launch a new scraping job."""
        height, width = self.stdscr.getmaxyx()

        # Check cookies first
        cookies_valid, cookies_msg = self.check_cookies_valid()

        if not cookies_valid:
            # Show warning and ask if user wants to run setup
            self.stdscr.clear()
            self.draw_header()

            y = 4
            self.stdscr.attron(curses.color_pair(7) | curses.A_BOLD)
            self.stdscr.addstr(y, 2, " ⚠ AVISO: Cookies no válidas ")
            self.stdscr.attroff(curses.color_pair(7) | curses.A_BOLD)

            y += 2
            self.stdscr.addstr(y, 2, f"Estado: {cookies_msg}")
            y += 2
            self.stdscr.addstr(y, 2, "Sin cookies válidas, el scraping no funcionará correctamente.")
            y += 1
            self.stdscr.addstr(y, 2, "Se recomienda ejecutar --setup primero para obtener cookies frescas.")

            y += 3
            self.stdscr.attron(curses.A_BOLD)
            self.stdscr.addstr(y, 2, "¿Qué deseas hacer?")
            self.stdscr.attroff(curses.A_BOLD)
            y += 2
            self.stdscr.addstr(y, 4, "[S] Ejecutar --setup ahora (abre navegador visible)")
            y += 1
            self.stdscr.addstr(y, 4, "[C] Continuar de todos modos (no recomendado)")
            y += 1
            self.stdscr.addstr(y, 4, "[Esc] Cancelar")

            self.stdscr.refresh()
            self.stdscr.timeout(-1)  # Block for input

            try:
                key = self.stdscr.getch()
                if key == ord('s') or key == ord('S'):
                    self.run_setup_mode()
                    # After setup, re-check cookies
                    cookies_valid, cookies_msg = self.check_cookies_valid()
                    if not cookies_valid:
                        self.show_message("Setup completado pero cookies aún no válidas. Inténtalo de nuevo.")
                        self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)
                        return
                elif key == ord('c') or key == ord('C'):
                    pass  # Continue anyway
                else:
                    self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)
                    return
            finally:
                self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)

        # Now show the new scraping form
        self.show_new_scraping_form()

    def show_selection_list(self, title: str, items: list, display_key: str = 'name',
                            allow_back: bool = False) -> tuple:
        """
        Shows a scrollable selection list and returns the selected item.

        Args:
            title: Title to show at the top
            items: List of dicts to choose from
            display_key: Key in each dict to display
            allow_back: If True, allows going back with 'B' key

        Returns:
            Tuple of (selected_item, action) where action is 'selected', 'skipped', 'back', or 'cancelled'
        """
        if not items:
            return None, 'skipped'

        height, width = self.stdscr.getmaxyx()
        selected = 0
        scroll_offset = 0
        max_visible = height - 10  # Leave room for header and footer

        while True:
            self.stdscr.clear()
            self.draw_header()

            y = 3
            self.stdscr.attron(curses.color_pair(4) | curses.A_BOLD)
            self.stdscr.addstr(y, 2, f"═══ {title} ═══")
            self.stdscr.attroff(curses.color_pair(4) | curses.A_BOLD)

            y += 2
            self.stdscr.addstr(y, 2, f"Selecciona una opción ({len(items)} disponibles):")
            y += 2

            # Calculate visible range
            if selected < scroll_offset:
                scroll_offset = selected
            elif selected >= scroll_offset + max_visible:
                scroll_offset = selected - max_visible + 1

            visible_items = items[scroll_offset:scroll_offset + max_visible]

            for i, item in enumerate(visible_items):
                actual_index = scroll_offset + i
                prefix = "→ " if actual_index == selected else "  "

                if actual_index == selected:
                    self.stdscr.attron(curses.A_REVERSE)

                display_text = item.get(display_key, str(item))
                # Truncate if too long
                max_text_len = width - 8
                if len(display_text) > max_text_len:
                    display_text = display_text[:max_text_len - 3] + "..."

                line = f"{prefix}{display_text}"
                try:
                    self.stdscr.addstr(y + i, 2, line[:width - 4])
                except curses.error:
                    pass

                if actual_index == selected:
                    self.stdscr.attroff(curses.A_REVERSE)

            # Scroll indicators
            if scroll_offset > 0:
                self.stdscr.addstr(y - 1, width - 10, "↑ más ↑")
            if scroll_offset + max_visible < len(items):
                self.stdscr.addstr(y + len(visible_items), width - 10, "↓ más ↓")

            # Footer
            footer_y = height - 3
            self.stdscr.addstr(footer_y, 2, "─" * (width - 4))
            self.stdscr.attron(curses.A_DIM)
            back_hint = "  [B] Volver" if allow_back else ""
            self.stdscr.addstr(footer_y + 1, 2, f"[↑/↓] Navegar  [Enter] Seleccionar  [Esc] Omitir{back_hint}")
            self.stdscr.attroff(curses.A_DIM)

            self.stdscr.refresh()
            self.stdscr.timeout(-1)
            key = self.stdscr.getch()

            if key == 27:  # Escape - skip this selection
                return None, 'skipped'
            elif allow_back and (key == ord('b') or key == ord('B')):
                return None, 'back'
            elif key == curses.KEY_UP or key == ord('k'):
                selected = max(0, selected - 1)
            elif key == curses.KEY_DOWN or key == ord('j'):
                selected = min(len(items) - 1, selected + 1)
            elif key == curses.KEY_PPAGE:  # Page Up
                selected = max(0, selected - max_visible)
            elif key == curses.KEY_NPAGE:  # Page Down
                selected = min(len(items) - 1, selected + max_visible)
            elif key == curses.KEY_HOME:
                selected = 0
            elif key == curses.KEY_END:
                selected = len(items) - 1
            elif key == ord('\n') or key == curses.KEY_ENTER:
                return items[selected], 'selected'

    def get_geonames_username(self) -> str:
        """Gets geonames username from setup_config.json or prompts for it."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        setup_config_path = os.path.join(script_dir, "browser_data", "setup_config.json")

        if os.path.exists(setup_config_path):
            try:
                with open(setup_config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    if config.get('geonames_username'):
                        return config['geonames_username']
            except:
                pass
        return 'demo'  # Fallback (limited API calls)

    def load_wizard_config(self) -> dict:
        """Loads the last wizard configuration from file."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        wizard_config_path = os.path.join(script_dir, "cache", "wizard_config.json")

        default_config = {
            'country_code': 'ES',
            'query': '',
            'min_population': 50000,
            'max_population': None,
            'strategy': 'simple',
            'max_results': 300,
            'workers': 1,
            'region_code': None,
            'region_name': None,
            'adm2_code': None,
            'adm2_name': None,
        }

        # Try wizard_config first (last wizard execution)
        if os.path.exists(wizard_config_path):
            try:
                with open(wizard_config_path, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
                    default_config.update(saved)
                    return default_config
            except:
                pass

        # Fallback to setup_config (from --setup)
        setup_config_path = os.path.join(script_dir, "browser_data", "setup_config.json")
        if os.path.exists(setup_config_path):
            try:
                with open(setup_config_path, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
                    default_config.update(saved)
            except:
                pass

        return default_config

    def save_wizard_config(self, config: dict):
        """Saves the wizard configuration for next time."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        wizard_config_path = os.path.join(script_dir, "cache", "wizard_config.json")

        try:
            # Only save relevant fields
            save_config = {
                'country_code': config.get('country_code'),
                'query': config.get('query'),
                'min_population': config.get('min_population'),
                'max_population': config.get('max_population'),
                'strategy': config.get('strategy'),
                'max_results': config.get('max_results'),
                'workers': config.get('workers'),
                'region_code': config.get('region_code'),
                'region_name': config.get('region_name'),
                'adm2_code': config.get('adm2_code'),
                'adm2_name': config.get('adm2_name'),
            }
            with open(wizard_config_path, 'w', encoding='utf-8') as f:
                json.dump(save_config, f, ensure_ascii=False, indent=2)
        except:
            pass  # Non-critical

    def show_new_scraping_form(self):
        """Shows the form to configure a new scraping job with region/subdivision support.

        Supports navigation back through phases with 'B' key.
        """
        height, width = self.stdscr.getmaxyx()
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Load configuration from last wizard or setup
        config = self.load_wizard_config()
        geonames_username = self.get_geonames_username()

        # State machine for phases
        phase = 1  # 1=basic, 2=region, 3=subdivision, 4=confirm

        # Cached data
        regions = []
        subdivisions = []
        final_config = {}

        while True:
            # ==================== PHASE 1: Basic Configuration ====================
            if phase == 1:
                fields = [
                    ('country_code', 'País (2 letras)', config.get('country_code', 'ES')),
                    ('query', 'Término de búsqueda', config.get('query', '')),
                    ('min_population', 'Población mínima', str(config.get('min_population', 50000))),
                    ('max_population', 'Población máxima (vacío=sin límite)', str(config.get('max_population') or '')),
                    ('workers', 'Workers paralelos (1-10)', str(config.get('workers', 1))),
                    ('strategy', 'Estrategia (simple/grid)', config.get('strategy', 'simple')),
                    ('max_results', 'Max resultados por celda', str(config.get('max_results', 300))),
                ]

                current_field = 0
                values = [f[2] for f in fields]

                curses.echo()
                curses.curs_set(1)

                phase1_done = False
                cancelled = False

                while not phase1_done and not cancelled:
                    self.stdscr.clear()
                    self.draw_header()

                    y = 3
                    self.stdscr.attron(curses.color_pair(4) | curses.A_BOLD)
                    self.stdscr.addstr(y, 2, "═══ NUEVO SCRAPING (1/3: Configuración básica) ═══")
                    self.stdscr.attroff(curses.color_pair(4) | curses.A_BOLD)

                    y += 2
                    self.stdscr.addstr(y, 2, "Completa los campos:")
                    y += 2

                    # Draw fields
                    for i, (key, label, _) in enumerate(fields):
                        prefix = "→ " if i == current_field else "  "
                        if i == current_field:
                            self.stdscr.attron(curses.A_BOLD)

                        field_line = f"{prefix}{label}: {values[i]}"
                        self.stdscr.addstr(y + i, 2, field_line[:width - 4])

                        if i == current_field:
                            self.stdscr.attroff(curses.A_BOLD)

                    # Instructions
                    y += len(fields) + 2
                    self.stdscr.addstr(y, 2, "─" * (width - 4))
                    y += 1
                    self.stdscr.attron(curses.A_DIM)
                    self.stdscr.addstr(y, 2, "[Tab/↓] Siguiente  [↑] Anterior  [Enter] Editar campo  [Esc] Cancelar")
                    y += 1
                    self.stdscr.addstr(y + 1, 2, "En el último campo, [Enter] avanza al siguiente paso.")
                    self.stdscr.attroff(curses.A_DIM)

                    self.stdscr.refresh()

                    # Position cursor
                    field_y = 7 + current_field
                    label = fields[current_field][1]
                    cursor_x = 4 + len(label) + 2 + len(values[current_field])
                    try:
                        self.stdscr.move(field_y, min(cursor_x, width - 2))
                    except curses.error:
                        pass

                    self.stdscr.timeout(-1)
                    key = self.stdscr.getch()

                    if key == 27:  # Escape - cancel
                        cancelled = True

                    elif key == curses.KEY_DOWN or key == ord('\t'):
                        current_field = (current_field + 1) % len(fields)

                    elif key == curses.KEY_UP or key == curses.KEY_BTAB:
                        current_field = (current_field - 1) % len(fields)

                    elif key == ord('\n') or key == curses.KEY_ENTER:
                        if current_field == len(fields) - 1:
                            # Last field - proceed
                            phase1_done = True
                        else:
                            # Edit current field
                            self.stdscr.addstr(field_y, 4 + len(fields[current_field][1]) + 2, " " * 40)
                            self.stdscr.move(field_y, 4 + len(fields[current_field][1]) + 2)
                            self.stdscr.refresh()

                            try:
                                input_bytes = self.stdscr.getstr(40)
                                new_value = input_bytes.decode('utf-8').strip()
                                if new_value:
                                    values[current_field] = new_value
                            except:
                                pass

                            current_field = (current_field + 1) % len(fields)

                    elif key == curses.KEY_BACKSPACE or key == 127 or key == 8:
                        if values[current_field]:
                            values[current_field] = values[current_field][:-1]

                    elif 32 <= key <= 126:
                        values[current_field] += chr(key)

                curses.noecho()
                curses.curs_set(0)

                if cancelled:
                    self.show_message("Cancelado")
                    self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)
                    return

                # Validate
                try:
                    final_config = {
                        'country_code': values[0].upper().strip(),
                        'query': values[1].strip(),
                        'min_population': int(values[2]) if values[2].strip() else 50000,
                        'max_population': int(values[3]) if values[3].strip() else None,
                        'workers': max(1, min(10, int(values[4]) if values[4].strip() else 1)),
                        'strategy': values[5].strip().lower() if values[5].strip() in ['simple', 'grid'] else 'simple',
                        'max_results': int(values[6]) if values[6].strip() else 300,
                        'region_code': None,
                        'region_name': None,
                        'adm2_code': None,
                        'adm2_name': None,
                    }

                    if len(final_config['country_code']) != 2:
                        self.show_message("Error: Código de país debe ser 2 letras")
                        continue

                    if not final_config['query']:
                        self.show_message("Error: El término de búsqueda es obligatorio")
                        continue

                    phase = 2  # Move to region selection

                except ValueError as e:
                    self.show_message(f"Error en valores: {e}")
                    continue

            # ==================== PHASE 2: Region Selection ====================
            elif phase == 2:
                # Fetch regions if not cached
                if not regions:
                    self.stdscr.clear()
                    self.draw_header()
                    self.stdscr.addstr(5, 2, f"Cargando regiones para {final_config['country_code']}...")
                    self.stdscr.refresh()

                    try:
                        regions = asyncio.run(get_regions_for_country(
                            final_config['country_code'],
                            geonames_username
                        ))
                    except Exception as e:
                        regions = []

                if not regions:
                    # No regions available, skip to confirmation
                    phase = 4
                    continue

                # Ask about region filtering
                self.stdscr.clear()
                self.draw_header()
                y = 5
                self.stdscr.attron(curses.color_pair(4) | curses.A_BOLD)
                self.stdscr.addstr(y, 2, "═══ NUEVO SCRAPING (2/3: Filtro por región) ═══")
                self.stdscr.attroff(curses.color_pair(4) | curses.A_BOLD)
                y += 2
                self.stdscr.addstr(y, 2, f"País: {final_config['country_code']} | {len(regions)} regiones disponibles")
                y += 2
                self.stdscr.addstr(y, 2, "¿Deseas filtrar por región específica?")
                y += 2
                self.stdscr.addstr(y, 4, "[S] Sí, seleccionar región")
                y += 1
                self.stdscr.addstr(y, 4, "[N] No, usar todo el país")
                y += 1
                self.stdscr.addstr(y, 4, "[B] Volver a configuración básica")
                y += 1
                self.stdscr.addstr(y, 4, "[Esc] Cancelar")
                self.stdscr.refresh()

                self.stdscr.timeout(-1)
                key = self.stdscr.getch()

                if key == 27:  # Escape
                    self.show_message("Cancelado")
                    self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)
                    return
                elif key == ord('b') or key == ord('B'):
                    phase = 1
                    regions = []  # Clear cache if going back
                    continue
                elif key == ord('n') or key == ord('N'):
                    phase = 4  # Skip to confirmation
                    continue
                elif key == ord('s') or key == ord('S'):
                    # Show region selection
                    selected_region, action = self.show_selection_list(
                        "SELECCIONAR REGIÓN",
                        regions,
                        'name',
                        allow_back=True
                    )

                    if action == 'back':
                        phase = 1
                        regions = []
                        continue
                    elif action == 'skipped':
                        phase = 4  # Skip to confirmation
                        continue
                    elif selected_region:
                        final_config['region_code'] = selected_region['code']
                        final_config['region_name'] = selected_region['name']
                        phase = 3  # Move to subdivision
                        subdivisions = []  # Clear cache
                    else:
                        phase = 4

            # ==================== PHASE 3: Subdivision Selection ====================
            elif phase == 3:
                # Fetch subdivisions if not cached
                if not subdivisions:
                    self.stdscr.clear()
                    self.draw_header()
                    self.stdscr.addstr(5, 2, f"Cargando subdivisiones para {final_config['region_name']}...")
                    self.stdscr.refresh()

                    try:
                        subdivisions = asyncio.run(get_subdivisions_for_region(
                            final_config['country_code'],
                            final_config['region_code'],
                            geonames_username
                        ))
                    except:
                        subdivisions = []

                if not subdivisions:
                    # No subdivisions, go to confirmation
                    phase = 4
                    continue

                # Ask about subdivision filtering
                self.stdscr.clear()
                self.draw_header()
                y = 5
                self.stdscr.attron(curses.color_pair(4) | curses.A_BOLD)
                self.stdscr.addstr(y, 2, "═══ NUEVO SCRAPING (3/3: Filtro por subdivisión) ═══")
                self.stdscr.attroff(curses.color_pair(4) | curses.A_BOLD)
                y += 2
                self.stdscr.addstr(y, 2, f"Región: {final_config['region_name']} | {len(subdivisions)} subdivisiones")
                y += 2
                self.stdscr.addstr(y, 2, "¿Deseas filtrar por subdivisión específica?")
                y += 2
                self.stdscr.addstr(y, 4, "[S] Sí, seleccionar subdivisión")
                y += 1
                self.stdscr.addstr(y, 4, "[N] No, usar toda la región")
                y += 1
                self.stdscr.addstr(y, 4, "[B] Volver a selección de región")
                y += 1
                self.stdscr.addstr(y, 4, "[Esc] Cancelar")
                self.stdscr.refresh()

                self.stdscr.timeout(-1)
                key = self.stdscr.getch()

                if key == 27:
                    self.show_message("Cancelado")
                    self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)
                    return
                elif key == ord('b') or key == ord('B'):
                    final_config['region_code'] = None
                    final_config['region_name'] = None
                    phase = 2
                    continue
                elif key == ord('n') or key == ord('N'):
                    phase = 4
                    continue
                elif key == ord('s') or key == ord('S'):
                    selected_sub, action = self.show_selection_list(
                        f"SUBDIVISIONES DE {final_config['region_name'].upper()}",
                        subdivisions,
                        'name',
                        allow_back=True
                    )

                    if action == 'back':
                        final_config['region_code'] = None
                        final_config['region_name'] = None
                        phase = 2
                        continue
                    elif selected_sub:
                        final_config['adm2_code'] = selected_sub['code']
                        final_config['adm2_name'] = selected_sub['name']

                    phase = 4

            # ==================== PHASE 4: Confirmation ====================
            elif phase == 4:
                self.stdscr.clear()
                self.draw_header()
                y = 4
                self.stdscr.attron(curses.color_pair(4) | curses.A_BOLD)
                self.stdscr.addstr(y, 2, "═══ CONFIRMAR CONFIGURACIÓN ═══")
                self.stdscr.attroff(curses.color_pair(4) | curses.A_BOLD)

                y += 2
                self.stdscr.addstr(y, 2, f"País: {final_config['country_code']}")
                y += 1
                if final_config.get('region_name'):
                    self.stdscr.addstr(y, 2, f"Región: {final_config['region_name']} ({final_config['region_code']})")
                    y += 1
                if final_config.get('adm2_name'):
                    self.stdscr.addstr(y, 2, f"Subdivisión: {final_config['adm2_name']} ({final_config['adm2_code']})")
                    y += 1
                self.stdscr.addstr(y, 2, f"Búsqueda: {final_config['query']}")
                y += 1
                pop_range = f"{final_config['min_population']:,}"
                if final_config.get('max_population'):
                    pop_range += f" - {final_config['max_population']:,}"
                else:
                    pop_range += "+"
                self.stdscr.addstr(y, 2, f"Población: {pop_range}")
                y += 1
                self.stdscr.addstr(y, 2, f"Estrategia: {final_config['strategy']} | Workers: {final_config['workers']} | Max resultados: {final_config['max_results']}")

                y += 3
                self.stdscr.attron(curses.A_BOLD)
                self.stdscr.addstr(y, 2, "¿Lanzar scraping?")
                self.stdscr.attroff(curses.A_BOLD)
                y += 2
                self.stdscr.addstr(y, 4, "[Enter] Sí, lanzar")
                y += 1
                self.stdscr.addstr(y, 4, "[B] Volver atrás")
                y += 1
                self.stdscr.addstr(y, 4, "[Esc] Cancelar")
                self.stdscr.refresh()

                self.stdscr.timeout(-1)
                key = self.stdscr.getch()

                if key == 27:
                    self.show_message("Cancelado")
                    self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)
                    return
                elif key == ord('b') or key == ord('B'):
                    # Go back to appropriate phase
                    if final_config.get('adm2_code'):
                        final_config['adm2_code'] = None
                        final_config['adm2_name'] = None
                        phase = 3
                    elif final_config.get('region_code'):
                        final_config['region_code'] = None
                        final_config['region_name'] = None
                        phase = 2
                    else:
                        phase = 2 if regions else 1
                    continue
                elif key == ord('\n') or key == curses.KEY_ENTER:
                    # Save config and launch
                    self.save_wizard_config(final_config)
                    self.launch_new_scraping(final_config)
                    self.stdscr.timeout(self.REFRESH_TIMEOUT_MS)
                    return

    def launch_new_scraping(self, config: dict):
        """Launches a new scraping job with the given configuration."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(script_dir, "scrape_maps_interactive.py")
        log_file = os.path.join(script_dir, "scrapping_background.log")

        # Build command
        cmd = [
            sys.executable, script_path,
            '--batch',
            '--country_code', config['country_code'],
            '--query', config['query'],
            '--min_population', str(config['min_population']),
            '--strategy', config['strategy'],
            '--max_results', str(config['max_results']),
            '--workers', str(config['workers']),
        ]

        if config.get('max_population'):
            cmd.extend(['--max_population', str(config['max_population'])])

        # Add region filter if specified
        if config.get('region_code'):
            cmd.extend(['--region_code', config['region_code']])

        # Add subdivision (ADM2) filter if specified
        if config.get('adm2_code'):
            cmd.extend(['--adm2_code', config['adm2_code']])

        try:
            with open(log_file, 'a') as lf:
                lf.write(f"\n{'='*60}\n")
                lf.write(f"Nuevo scraping lanzado desde manager\n")
                lf.write(f"Config: {json.dumps(config, ensure_ascii=False)}\n")
                lf.write(f"Timestamp: {datetime.now().isoformat()}\n")
                lf.write(f"{'='*60}\n")

                proc = subprocess.Popen(
                    cmd,
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                    start_new_session=True
                )

            self.show_message(f"Scraping lanzado con PID {proc.pid}", 5.0)
            self.view_mode = "active"  # Switch to active view

        except Exception as e:
            self.show_message(f"Error lanzando scraping: {e}")

    def update_pending_commands(self):
        """Updates pending commands based on actual instance status.

        Commands persist until the expected status is reached, not just timeout.
        """
        instances = self.job_manager.list_instances()
        instance_map = {i.pid: i for i in instances}

        to_remove = []
        for pid, cmd_tuple in self.pending_commands.items():
            cmd, ts, _ = cmd_tuple
            if pid not in instance_map:
                # Instance gone, remove pending command
                to_remove.append(pid)
            else:
                instance = instance_map[pid]
                # Check if expected status reached
                if cmd == "PAUSING" and instance.status == JobStatus.PAUSED:
                    to_remove.append(pid)
                elif cmd == "RESUMING" and instance.status == JobStatus.RUNNING:
                    to_remove.append(pid)
                elif cmd == "STOPPING" and instance.status in [JobStatus.STOPPING, JobStatus.COMPLETED, JobStatus.INTERRUPTED]:
                    to_remove.append(pid)
                elif cmd.startswith("W=") and time.time() - ts > 5:
                    # Worker changes clear after 5 seconds
                    to_remove.append(pid)
                # Note: No timeout for PAUSING/STOPPING - they persist until status changes

        for pid in to_remove:
            del self.pending_commands[pid]

    def run(self):
        """Main loop.

        Optimizado para reducir I/O:
        - Timeout de getch en 500ms (antes 100ms)
        - Sin sleep adicional entre frames
        - El JobManager ya tiene cache con TTL
        """
        while self.running:
            self.stdscr.clear()

            self.update_pending_commands()
            self.draw_header()

            if self.view_mode == "active":
                self.draw_active_instances()
            elif self.view_mode == "history":
                self.draw_history()
            else:
                self.draw_files_view()

            self.draw_help_bar()
            self.stdscr.refresh()

            try:
                key = self.stdscr.getch()
                if key != -1:
                    self.handle_input(key)
            except curses.error:
                pass

            # Sin sleep adicional - el timeout de getch ya controla la frecuencia
            if self.SLEEP_BETWEEN_FRAMES > 0:
                time.sleep(self.SLEEP_BETWEEN_FRAMES)


def main(stdscr):
    """Entry point for curses wrapper."""
    manager = ScrapeManager(stdscr)
    manager.run()


if __name__ == "__main__":
    try:
        curses.wrapper(main)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
