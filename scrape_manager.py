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
from datetime import datetime
from typing import List, Optional
from collections import deque

from job_manager import (
    JobManager, Job, Instance, JobStatus, JobStats,
    ControlCommand, format_duration, calculate_eta
)


class ScrapeManager:
    """Interfaz curses para gestionar instancias del scraper."""

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

        self.stdscr.timeout(100)  # Non-blocking getch with 100ms timeout

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
            help_text = " [P]ausar [R]esumir [S]top [K]ill [W]orkers [F]iles [1]Act [2]Hist [Q]uit "
        elif self.view_mode == "history":
            help_text = " [Enter]Revivir [D]eliminar [F]iles [1]Activas [2]Hist [Q]uit "
        else:  # files
            email_mode = "Todos" if self.show_all_emails else "Corp"
            help_text = f" [↑↓]Scroll [E]mails:{email_mode} [1]Activas [2]Histórico [Q]uit "

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
        if start_y >= height - 8:
            return

        stats = instance.stats
        config = instance.config
        job = self.job_manager.load_job(instance.job_id)

        self.stdscr.addstr(start_y, 0, "=" * (width - 1))

        # Title
        self.stdscr.attron(curses.color_pair(4) | curses.A_BOLD)
        title = f" Detalles - PID {instance.pid} | {config.get('query', 'N/A')} en {config.get('country_code', 'N/A')} "
        self.stdscr.addstr(start_y + 1, 2, title[:width - 4])
        self.stdscr.attroff(curses.color_pair(4) | curses.A_BOLD)

        # Progress section
        y = start_y + 3
        cities_remaining = stats.cities_total - stats.cities_processed
        progress_pct = (stats.cities_processed / stats.cities_total * 100) if stats.cities_total > 0 else 0

        # Current city info
        if stats.current_city:
            city_info = f"Ciudad actual: {stats.current_city}"
            if stats.current_city_population > 0:
                city_info += f" ({self.format_population(stats.current_city_population)} hab)"
            if stats.current_city_index > 0:
                city_info += f" [{stats.current_city_index}/{stats.cities_total}]"
            self.stdscr.addstr(y, 4, city_info[:width - 6])
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
        """Counts lines in a CSV file (excluding header)."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f) - 1  # Exclude header
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
            self.stdscr.timeout(100)

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

            # Find and update the checkpoint file
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cache_dir = os.path.join(script_dir, "cache")

            # Find checkpoint for this job
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
                    # Check if process is still alive
                    try:
                        os.kill(pid, 0)
                    except OSError:
                        # Process is dead
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
                    lf.write(f"Timestamp: {datetime.now().isoformat()}\n")
                    lf.write(f"{'='*60}\n")
                    subprocess.Popen(
                        [sys.executable, script_path, '--resume', '--batch'],
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
            self.stdscr.timeout(100)

    def revive_job(self, job: Job):
        """Revives an interrupted job by launching a new process.

        The old job entry is deleted from history to avoid duplicates.
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(script_dir, "scrape_maps_interactive.py")

        cmd_parts = [
            sys.executable, script_path,
            '--resume',
            '--batch'
        ]

        log_file = os.path.join(script_dir, "scrapping_background.log")

        try:
            with open(log_file, 'a') as lf:
                lf.write(f"\n{'='*60}\n")
                lf.write(f"Reviviendo job: {job.job_id}\n")
                lf.write(f"Timestamp: {datetime.now().isoformat()}\n")
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

            self.show_message(f"Job revivido con PID {proc.pid}", 5.0)
        except Exception as e:
            self.show_message(f"Error reviviendo job: {e}")

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
        """Main loop."""
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

            time.sleep(0.05)


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
