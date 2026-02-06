"""
Job Manager - Sistema de gestión de trabajos e instancias para el scraper.

Proporciona:
- Persistencia de jobs (histórico de trabajos)
- Registro de instancias vivas
- Control de instancias (pause, resume, stop, workers)
- Métricas en tiempo real
"""

import os
import json
import threading
import time
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List
from enum import Enum


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    FAILED = "failed"


class ControlCommand(str, Enum):
    PAUSE = "pause"
    RESUME = "resume"
    STOP = "stop"
    SET_WORKERS = "set_workers"


@dataclass
class JobStats:
    """Estadísticas de un trabajo."""
    cities_total: int = 0
    cities_processed: int = 0
    results_total: int = 0
    results_with_email: int = 0
    results_with_corporate_email: int = 0
    results_with_web: int = 0
    runtime_seconds: float = 0
    current_city: str = ""
    current_city_index: int = 0  # Posición actual (1-based)
    current_city_population: int = 0
    current_city_start: Optional[str] = None  # ISO timestamp
    last_city_duration: float = 0  # Segundos que tardó la última ciudad
    avg_city_duration: float = 0  # Media de segundos por ciudad
    errors_count: int = 0


@dataclass
class Job:
    """Representa un trabajo de scraping."""
    job_id: str
    created_at: str  # ISO timestamp
    config: Dict[str, Any]
    status: str = JobStatus.PENDING
    finished_at: Optional[str] = None
    stats: JobStats = field(default_factory=JobStats)
    checkpoint_file: Optional[str] = None
    csv_with_emails: Optional[str] = None
    csv_no_emails: Optional[str] = None
    last_pid: Optional[int] = None
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        """Convierte a diccionario para serialización JSON."""
        data = {
            'job_id': self.job_id,
            'created_at': self.created_at,
            'finished_at': self.finished_at,
            'status': self.status,
            'config': self.config,
            'stats': asdict(self.stats) if isinstance(self.stats, JobStats) else self.stats,
            'checkpoint_file': self.checkpoint_file,
            'csv_with_emails': self.csv_with_emails,
            'csv_no_emails': self.csv_no_emails,
            'last_pid': self.last_pid,
            'error_message': self.error_message,
        }
        return data

    @classmethod
    def from_dict(cls, data: dict) -> 'Job':
        """Crea un Job desde un diccionario."""
        stats_data = data.get('stats', {})
        if isinstance(stats_data, dict):
            stats = JobStats(**stats_data)
        else:
            stats = stats_data

        return cls(
            job_id=data['job_id'],
            created_at=data['created_at'],
            finished_at=data.get('finished_at'),
            status=data.get('status', JobStatus.PENDING),
            config=data['config'],
            stats=stats,
            checkpoint_file=data.get('checkpoint_file'),
            csv_with_emails=data.get('csv_with_emails'),
            csv_no_emails=data.get('csv_no_emails'),
            last_pid=data.get('last_pid'),
            error_message=data.get('error_message'),
        )


@dataclass
class Instance:
    """Representa una instancia viva del scraper."""
    pid: int
    job_id: str
    started_at: str  # ISO timestamp
    status: str = JobStatus.RUNNING
    stats: JobStats = field(default_factory=JobStats)
    config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'pid': self.pid,
            'job_id': self.job_id,
            'started_at': self.started_at,
            'status': self.status,
            'stats': asdict(self.stats) if isinstance(self.stats, JobStats) else self.stats,
            'config': self.config,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Instance':
        stats_data = data.get('stats', {})
        if isinstance(stats_data, dict):
            stats = JobStats(**stats_data)
        else:
            stats = stats_data

        return cls(
            pid=data['pid'],
            job_id=data['job_id'],
            started_at=data['started_at'],
            status=data.get('status', JobStatus.RUNNING),
            stats=stats,
            config=data.get('config', {}),
        )


class JobManager:
    """Gestiona la persistencia de jobs e instancias."""

    def __init__(self, base_dir: str = None):
        if base_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        self.base_dir = base_dir
        self.cache_dir = os.path.join(base_dir, "cache")
        self.jobs_dir = os.path.join(self.cache_dir, "jobs")
        self.instances_dir = os.path.join(self.cache_dir, "instances")
        self.control_dir = os.path.join(self.cache_dir, "control")

        # Crear directorios
        for d in [self.jobs_dir, self.instances_dir, self.control_dir]:
            os.makedirs(d, exist_ok=True)

    def _job_path(self, job_id: str) -> str:
        return os.path.join(self.jobs_dir, f"{job_id}.json")

    def _instance_path(self, pid: int) -> str:
        return os.path.join(self.instances_dir, f"{pid}.json")

    def _control_path(self, pid: int) -> str:
        return os.path.join(self.control_dir, f"{pid}.json")

    # --- Jobs ---

    def create_job(self, config: Dict[str, Any]) -> Job:
        """Crea un nuevo job y lo guarda."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        country = config.get('country_code', 'XX')
        query = config.get('query', 'unknown').replace(' ', '_')[:20]
        job_id = f"job_{timestamp}_{country}_{query}"

        job = Job(
            job_id=job_id,
            created_at=datetime.now().isoformat(),
            config=config,
            status=JobStatus.PENDING,
        )
        self.save_job(job)
        return job

    def save_job(self, job: Job) -> None:
        """Guarda un job a disco."""
        with open(self._job_path(job.job_id), 'w', encoding='utf-8') as f:
            json.dump(job.to_dict(), f, ensure_ascii=False, indent=2)

    def load_job(self, job_id: str) -> Optional[Job]:
        """Carga un job desde disco."""
        path = self._job_path(job_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                if not content.strip():
                    # Empty file - delete it
                    os.remove(path)
                    return None
                return Job.from_dict(json.loads(content))
        except (json.JSONDecodeError, KeyError, ValueError):
            # Corrupted file - delete it
            try:
                os.remove(path)
            except:
                pass
            return None

    def list_jobs(self) -> List[Job]:
        """Lista todos los jobs."""
        jobs = []
        for filename in os.listdir(self.jobs_dir):
            if filename.endswith('.json'):
                job_id = filename[:-5]
                try:
                    job = self.load_job(job_id)
                    if job:
                        jobs.append(job)
                except Exception:
                    # Skip corrupted files
                    pass
        # Ordenar por fecha de creación (más reciente primero)
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs

    def delete_job(self, job_id: str) -> bool:
        """Elimina un job."""
        path = self._job_path(job_id)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    # --- Instances ---

    def register_instance(self, pid: int, job: Job, config: Dict[str, Any]) -> Instance:
        """Registra una nueva instancia viva."""
        instance = Instance(
            pid=pid,
            job_id=job.job_id,
            started_at=datetime.now().isoformat(),
            status=JobStatus.RUNNING,
            config=config,
        )
        self.save_instance(instance)

        # Actualizar el job con el PID
        job.last_pid = pid
        job.status = JobStatus.RUNNING
        self.save_job(job)

        return instance

    def save_instance(self, instance: Instance) -> None:
        """Guarda una instancia a disco."""
        with open(self._instance_path(instance.pid), 'w', encoding='utf-8') as f:
            json.dump(instance.to_dict(), f, ensure_ascii=False, indent=2)

    def load_instance(self, pid: int) -> Optional[Instance]:
        """Carga una instancia desde disco."""
        path = self._instance_path(pid)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return Instance.from_dict(json.load(f))
        except (json.JSONDecodeError, KeyError):
            return None

    def unregister_instance(self, pid: int) -> None:
        """Elimina el registro de una instancia (al terminar)."""
        path = self._instance_path(pid)
        if os.path.exists(path):
            os.remove(path)
        # También eliminar el control file
        ctrl_path = self._control_path(pid)
        if os.path.exists(ctrl_path):
            os.remove(ctrl_path)

    def list_instances(self) -> List[Instance]:
        """Lista todas las instancias registradas."""
        instances = []
        for filename in os.listdir(self.instances_dir):
            if filename.endswith('.json'):
                try:
                    pid = int(filename[:-5])
                    instance = self.load_instance(pid)
                    if instance:
                        instances.append(instance)
                except ValueError:
                    continue
        return instances

    def is_instance_alive(self, pid: int) -> bool:
        """Verifica si un proceso sigue vivo."""
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def cleanup_dead_instances(self) -> List[int]:
        """Limpia instancias muertas y marca sus jobs como interrupted."""
        dead_pids = []
        for instance in self.list_instances():
            if not self.is_instance_alive(instance.pid):
                dead_pids.append(instance.pid)
                # Marcar el job como interrupted
                job = self.load_job(instance.job_id)
                if job and job.status == JobStatus.RUNNING:
                    job.status = JobStatus.INTERRUPTED
                    job.finished_at = datetime.now().isoformat()
                    self.save_job(job)
                self.unregister_instance(instance.pid)
        return dead_pids

    # --- Control ---

    def send_command(self, pid: int, command: str, value: Any = None) -> bool:
        """Envía un comando a una instancia."""
        instance = self.load_instance(pid)
        if not instance:
            return False

        ctrl_data = {
            'command': command,
            'value': value,
            'timestamp': datetime.now().isoformat(),
        }
        with open(self._control_path(pid), 'w', encoding='utf-8') as f:
            json.dump(ctrl_data, f)
        return True

    def read_command(self, pid: int) -> Optional[Dict[str, Any]]:
        """Lee y consume un comando pendiente."""
        path = self._control_path(pid)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # Consumir el comando (eliminar el fichero)
            os.remove(path)
            return data
        except (json.JSONDecodeError, FileNotFoundError):
            return None

    def peek_command(self, pid: int) -> Optional[Dict[str, Any]]:
        """Lee un comando sin consumirlo."""
        path = self._control_path(pid)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return None


class ControlThread(threading.Thread):
    """
    Thread que lee comandos de control cada N segundos y setea flags atómicos.
    """

    def __init__(self, pid: int, job_manager: JobManager, poll_interval: float = 1.0):
        super().__init__(daemon=True)
        self.pid = pid
        self.job_manager = job_manager
        self.poll_interval = poll_interval

        # Flags atómicos (thread-safe en Python para bool/int simples)
        self.pause_flag = False
        self.stop_flag = False
        self.new_workers: Optional[int] = None

        self._running = True

    def run(self):
        while self._running:
            try:
                cmd = self.job_manager.read_command(self.pid)
                if cmd:
                    command = cmd.get('command')
                    value = cmd.get('value')

                    if command == ControlCommand.PAUSE:
                        self.pause_flag = True
                    elif command == ControlCommand.RESUME:
                        self.pause_flag = False
                    elif command == ControlCommand.STOP:
                        self.stop_flag = True
                    elif command == ControlCommand.SET_WORKERS:
                        if isinstance(value, int) and value > 0:
                            self.new_workers = value
            except Exception:
                pass  # Ignorar errores de lectura

            time.sleep(self.poll_interval)

    def stop(self):
        self._running = False

    def consume_new_workers(self) -> Optional[int]:
        """Consume y retorna el nuevo número de workers, o None."""
        val = self.new_workers
        self.new_workers = None
        return val


class StatsThread(threading.Thread):
    """
    Thread que escribe estadísticas a disco periódicamente.
    """

    def __init__(self, pid: int, job: Job, job_manager: JobManager,
                 update_interval: float = 5.0):
        super().__init__(daemon=True)
        self.pid = pid
        self.job = job
        self.job_manager = job_manager
        self.update_interval = update_interval

        # Stats compartidas (el worker las actualiza)
        self.stats = JobStats()
        self.status = JobStatus.RUNNING
        self.start_time = time.time()

        self._running = True
        self._lock = threading.Lock()

    def update_stats(self, **kwargs):
        """Actualiza estadísticas de forma thread-safe."""
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self.stats, key):
                    setattr(self.stats, key, value)

    def set_status(self, status: str):
        """Actualiza el estado."""
        with self._lock:
            self.status = status

    def run(self):
        while self._running:
            try:
                with self._lock:
                    # Actualizar runtime
                    self.stats.runtime_seconds = time.time() - self.start_time

                    # Guardar instancia
                    instance = self.job_manager.load_instance(self.pid)
                    if instance:
                        instance.stats = JobStats(**asdict(self.stats))
                        instance.status = self.status
                        self.job_manager.save_instance(instance)

                    # Guardar job
                    self.job.stats = JobStats(**asdict(self.stats))
                    self.job.status = self.status
                    self.job_manager.save_job(self.job)
            except Exception:
                pass  # Ignorar errores de escritura

            time.sleep(self.update_interval)

    def stop(self):
        self._running = False

    def get_stats(self) -> JobStats:
        """Obtiene una copia de las stats actuales."""
        with self._lock:
            return JobStats(**asdict(self.stats))


def format_duration(seconds: float) -> str:
    """Formatea duración en formato legible."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def calculate_eta(stats: JobStats) -> str:
    """Calcula el tiempo estimado restante."""
    if stats.cities_processed == 0:
        return "Calculando..."

    avg_time = stats.runtime_seconds / stats.cities_processed
    remaining = stats.cities_total - stats.cities_processed
    eta_seconds = avg_time * remaining

    return format_duration(eta_seconds)
