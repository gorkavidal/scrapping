"""
Job Manager - Sistema de gestión de trabajos e instancias para el scraper.

Proporciona:
- Persistencia de jobs (histórico de trabajos)
- Registro de instancias vivas
- Control de instancias (pause, resume, stop, workers)
- Métricas en tiempo real

Optimizaciones v2:
- Escritura atómica de JSON (write to temp + rename)
- Cache con TTL para reducir I/O
- Debounce de cleanup
- Validación JSON robusta
"""

import os
import json
import platform
import threading
import time
import tempfile
import logging
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List, Tuple
from enum import Enum

# Windows compatibility
IS_WINDOWS = platform.system() == 'Windows'


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
    RETRY_CITY = "retry_city"  # Reiniciar ciudad atascada


@dataclass
class CityProgress:
    """Progreso de una ciudad en proceso."""
    name: str = ""
    population: int = 0
    index: int = 0  # Posición en la cola (1-based)
    start_time: Optional[str] = None  # ISO timestamp
    businesses_total: int = 0  # Negocios encontrados en Google Maps
    businesses_processed: int = 0  # Negocios con email ya extraído
    worker_id: int = 0  # ID del worker que la procesa


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
    # Campos legacy para compatibilidad (usados cuando hay 1 worker)
    current_city: str = ""
    current_city_index: int = 0  # Posición actual (1-based)
    current_city_population: int = 0
    current_city_start: Optional[str] = None  # ISO timestamp
    last_city_duration: float = 0  # Segundos que tardó la última ciudad
    avg_city_duration: float = 0  # Media de segundos por ciudad
    errors_count: int = 0
    # Progreso granular - legacy para 1 worker
    current_city_businesses_total: int = 0  # Negocios encontrados en Google Maps
    current_city_businesses_processed: int = 0  # Negocios con email ya extraído
    # Nuevo: Diccionario de ciudades activas para múltiples workers
    # Clave: nombre de ciudad, Valor: dict con progreso
    active_cities: Dict[str, Dict[str, Any]] = field(default_factory=dict)


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


@dataclass
class CacheEntry:
    """Entrada de cache con TTL."""
    data: Any
    mtime: float  # Modification time del archivo
    cached_at: float  # Cuándo se cacheó


class JobManager:
    """Gestiona la persistencia de jobs e instancias.

    Optimizaciones:
    - Cache con TTL para reducir lecturas de disco
    - Escritura atómica para evitar corrupción
    - Debounce de cleanup para no ejecutar cada frame
    """

    # TTL de cache en segundos
    CACHE_TTL = 2.0
    # Intervalo mínimo entre cleanups
    CLEANUP_DEBOUNCE = 5.0

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

        # Cache para reducir I/O
        self._job_cache: Dict[str, CacheEntry] = {}
        self._instance_cache: Dict[int, CacheEntry] = {}
        self._instances_list_cache: Optional[Tuple[float, List[Instance]]] = None
        self._jobs_list_cache: Optional[Tuple[float, List[Job]]] = None

        # Debounce para cleanup
        self._last_cleanup_time = 0.0

    def _job_path(self, job_id: str) -> str:
        return os.path.join(self.jobs_dir, f"{job_id}.json")

    def _instance_path(self, pid: int) -> str:
        return os.path.join(self.instances_dir, f"{pid}.json")

    def _control_path(self, pid: int) -> str:
        return os.path.join(self.control_dir, f"{pid}.json")

    def _atomic_write_json(self, path: str, data: dict) -> bool:
        """Escribe JSON de forma atómica: temp file + rename.

        Esto evita que lecturas concurrentes lean archivos parcialmente escritos.
        """
        dir_path = os.path.dirname(path)
        try:
            # Crear archivo temporal en el mismo directorio (para que rename sea atómico)
            fd, temp_path = tempfile.mkstemp(suffix='.tmp', dir=dir_path)
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                # Rename atómico (en el mismo filesystem)
                os.replace(temp_path, path)
                return True
            except Exception:
                # Limpiar archivo temporal si algo falla
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
                raise
        except Exception:
            return False

    def _safe_read_json(self, path: str) -> Optional[dict]:
        """Lee JSON con validación robusta.

        Retorna None si el archivo no existe, está vacío, o es inválido.
        No elimina archivos corruptos automáticamente para evitar race conditions.
        """
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            if not content or not content.strip():
                return None
            data = json.loads(content)
            if not isinstance(data, dict):
                return None
            return data
        except (json.JSONDecodeError, IOError, OSError):
            return None

    def _is_cache_valid(self, cache_entry: Optional[CacheEntry], path: str) -> bool:
        """Verifica si una entrada de cache sigue siendo válida."""
        if cache_entry is None:
            return False
        # Verificar TTL
        if time.time() - cache_entry.cached_at > self.CACHE_TTL:
            return False
        # Verificar si el archivo ha cambiado
        try:
            current_mtime = os.path.getmtime(path)
            return current_mtime == cache_entry.mtime
        except OSError:
            return False

    def invalidate_cache(self, job_id: str = None, pid: int = None):
        """Invalida entradas específicas del cache."""
        if job_id and job_id in self._job_cache:
            del self._job_cache[job_id]
        if pid and pid in self._instance_cache:
            del self._instance_cache[pid]
        # Invalidar listas
        self._instances_list_cache = None
        self._jobs_list_cache = None

    def invalidate_all_cache(self):
        """Invalida todo el cache."""
        self._job_cache.clear()
        self._instance_cache.clear()
        self._instances_list_cache = None
        self._jobs_list_cache = None

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
        """Guarda un job a disco de forma atómica."""
        path = self._job_path(job.job_id)
        if self._atomic_write_json(path, job.to_dict()):
            # Actualizar cache
            try:
                mtime = os.path.getmtime(path)
                self._job_cache[job.job_id] = CacheEntry(
                    data=job,
                    mtime=mtime,
                    cached_at=time.time()
                )
            except OSError:
                pass
        # Invalidar lista de jobs
        self._jobs_list_cache = None

    def load_job(self, job_id: str) -> Optional[Job]:
        """Carga un job desde disco (con cache)."""
        path = self._job_path(job_id)

        # Verificar cache
        cache_entry = self._job_cache.get(job_id)
        if self._is_cache_valid(cache_entry, path):
            return cache_entry.data

        # Leer de disco
        data = self._safe_read_json(path)
        if data is None:
            # Archivo no existe o corrupto, limpiar cache
            if job_id in self._job_cache:
                del self._job_cache[job_id]
            return None

        try:
            job = Job.from_dict(data)
            # Actualizar cache
            try:
                mtime = os.path.getmtime(path)
                self._job_cache[job_id] = CacheEntry(
                    data=job,
                    mtime=mtime,
                    cached_at=time.time()
                )
            except OSError:
                pass
            return job
        except (KeyError, ValueError, TypeError):
            return None

    def list_jobs(self) -> List[Job]:
        """Lista todos los jobs (con cache de lista)."""
        # Verificar cache de lista
        if self._jobs_list_cache is not None:
            cache_time, cached_list = self._jobs_list_cache
            if time.time() - cache_time < self.CACHE_TTL:
                return cached_list

        jobs = []
        try:
            filenames = os.listdir(self.jobs_dir)
        except OSError:
            return []

        for filename in filenames:
            if filename.endswith('.json'):
                job_id = filename[:-5]
                try:
                    job = self.load_job(job_id)
                    if job:
                        jobs.append(job)
                except Exception:
                    pass

        # Ordenar por fecha de creación (más reciente primero)
        jobs.sort(key=lambda j: j.created_at, reverse=True)

        # Guardar en cache
        self._jobs_list_cache = (time.time(), jobs)
        return jobs

    def delete_job(self, job_id: str) -> bool:
        """Elimina un job."""
        path = self._job_path(job_id)
        if os.path.exists(path):
            try:
                os.remove(path)
                self.invalidate_cache(job_id=job_id)
                return True
            except OSError:
                return False
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
        """Guarda una instancia a disco de forma atómica."""
        path = self._instance_path(instance.pid)
        if self._atomic_write_json(path, instance.to_dict()):
            # Actualizar cache
            try:
                mtime = os.path.getmtime(path)
                self._instance_cache[instance.pid] = CacheEntry(
                    data=instance,
                    mtime=mtime,
                    cached_at=time.time()
                )
            except OSError:
                pass
        # Invalidar lista
        self._instances_list_cache = None

    def load_instance(self, pid: int) -> Optional[Instance]:
        """Carga una instancia desde disco (con cache)."""
        path = self._instance_path(pid)

        # Verificar cache
        cache_entry = self._instance_cache.get(pid)
        if self._is_cache_valid(cache_entry, path):
            return cache_entry.data

        # Leer de disco
        data = self._safe_read_json(path)
        if data is None:
            if pid in self._instance_cache:
                del self._instance_cache[pid]
            return None

        try:
            instance = Instance.from_dict(data)
            # Actualizar cache
            try:
                mtime = os.path.getmtime(path)
                self._instance_cache[pid] = CacheEntry(
                    data=instance,
                    mtime=mtime,
                    cached_at=time.time()
                )
            except OSError:
                pass
            return instance
        except (KeyError, ValueError, TypeError):
            return None

    def unregister_instance(self, pid: int) -> None:
        """Elimina el registro de una instancia (al terminar)."""
        path = self._instance_path(pid)
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
        # También eliminar el control file
        ctrl_path = self._control_path(pid)
        if os.path.exists(ctrl_path):
            try:
                os.remove(ctrl_path)
            except OSError:
                pass
        self.invalidate_cache(pid=pid)

    def list_instances(self) -> List[Instance]:
        """Lista todas las instancias registradas (con cache de lista)."""
        # Verificar cache de lista
        if self._instances_list_cache is not None:
            cache_time, cached_list = self._instances_list_cache
            if time.time() - cache_time < self.CACHE_TTL:
                return cached_list

        instances = []
        try:
            filenames = os.listdir(self.instances_dir)
        except OSError:
            return []

        for filename in filenames:
            if filename.endswith('.json'):
                try:
                    pid = int(filename[:-5])
                    instance = self.load_instance(pid)
                    if instance:
                        instances.append(instance)
                except (ValueError, TypeError):
                    continue

        # Guardar en cache
        self._instances_list_cache = (time.time(), instances)
        return instances

    def is_instance_alive(self, pid: int) -> bool:
        """Verifica si un proceso sigue vivo.

        En Windows, os.kill(pid, 0) no funciona igual que en Unix.
        Usamos ctypes para verificar el proceso en Windows.
        """
        if IS_WINDOWS:
            # En Windows, usar OpenProcess para verificar si el proceso existe
            import ctypes
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            STILL_ACTIVE = 259

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if handle == 0:
                return False

            # Verificar si el proceso sigue activo
            exit_code = ctypes.c_ulong()
            result = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
            kernel32.CloseHandle(handle)

            if result == 0:
                return False
            return exit_code.value == STILL_ACTIVE
        else:
            # Unix: os.kill con señal 0 verifica si el proceso existe
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False

    def cleanup_dead_instances(self) -> List[int]:
        """Limpia instancias muertas y marca sus jobs como interrupted.

        Incluye debounce para no ejecutar en cada frame.
        """
        now = time.time()
        # Debounce: no ejecutar más de una vez cada CLEANUP_DEBOUNCE segundos
        if now - self._last_cleanup_time < self.CLEANUP_DEBOUNCE:
            return []
        self._last_cleanup_time = now

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
        """Envía un comando a una instancia (escritura atómica)."""
        instance = self.load_instance(pid)
        if not instance:
            return False

        ctrl_data = {
            'command': command,
            'value': value,
            'timestamp': datetime.now().isoformat(),
        }
        return self._atomic_write_json(self._control_path(pid), ctrl_data)

    def read_command(self, pid: int) -> Optional[Dict[str, Any]]:
        """Lee y consume un comando pendiente."""
        path = self._control_path(pid)
        data = self._safe_read_json(path)
        if data is not None:
            # Consumir el comando (eliminar el fichero)
            try:
                os.remove(path)
            except OSError:
                pass
        return data

    def peek_command(self, pid: int) -> Optional[Dict[str, Any]]:
        """Lee un comando sin consumirlo."""
        return self._safe_read_json(self._control_path(pid))


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
        self.retry_city: Optional[str] = None  # Nombre de ciudad a reiniciar

        self._running = True

    def run(self):
        logging.debug(f"[ControlThread] Iniciado para PID {self.pid}")
        iteration = 0
        while self._running:
            iteration += 1
            try:
                cmd = self.job_manager.read_command(self.pid)
                if cmd:
                    command = cmd.get('command')
                    value = cmd.get('value')
                    logging.info(f"[ControlThread] Comando recibido: {command} (value={value})")

                    if command == ControlCommand.PAUSE or command == 'pause':
                        self.pause_flag = True
                        logging.info(f"[ControlThread] pause_flag = True")
                    elif command == ControlCommand.RESUME or command == 'resume':
                        self.pause_flag = False
                        logging.info(f"[ControlThread] pause_flag = False")
                    elif command == ControlCommand.STOP or command == 'stop':
                        self.stop_flag = True
                        logging.info(f"[ControlThread] stop_flag = True")
                    elif command == ControlCommand.SET_WORKERS or command == 'set_workers':
                        if isinstance(value, int) and value > 0:
                            self.new_workers = value
                            logging.info(f"[ControlThread] new_workers = {value}")
                    elif command == ControlCommand.RETRY_CITY or command == 'retry_city':
                        if isinstance(value, str) and value:
                            self.retry_city = value
                            logging.info(f"[ControlThread] retry_city = {value}")
                    else:
                        logging.warning(f"[ControlThread] Comando desconocido: {command}")
            except Exception as e:
                logging.debug(f"[ControlThread] Error en iteración {iteration}: {e}")

            time.sleep(self.poll_interval)
        logging.debug(f"[ControlThread] Terminado para PID {self.pid}")

    def stop(self):
        self._running = False

    def consume_new_workers(self) -> Optional[int]:
        """Consume y retorna el nuevo número de workers, o None."""
        val = self.new_workers
        self.new_workers = None
        return val

    def consume_retry_city(self) -> Optional[str]:
        """Consume y retorna el nombre de ciudad a reiniciar, o None."""
        val = self.retry_city
        self.retry_city = None
        return val


class StatsThread(threading.Thread):
    """
    Thread que escribe estadísticas a disco periódicamente.

    Soporta stats acumuladas de ejecuciones anteriores (para --resume).
    """

    def __init__(self, pid: int, job: Job, job_manager: JobManager,
                 update_interval: float = 5.0, base_stats: JobStats = None):
        super().__init__(daemon=True)
        self.pid = pid
        self.job = job
        self.job_manager = job_manager
        self.update_interval = update_interval

        # Stats compartidas (el worker las actualiza)
        # Si hay base_stats (de un resume), usarlas como punto de partida
        if base_stats:
            self.stats = base_stats
            self.base_runtime = base_stats.runtime_seconds  # Tiempo acumulado anterior
        else:
            self.stats = JobStats()
            self.base_runtime = 0.0

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

    def add_active_city(self, city_name: str, worker_id: int, population: int = 0, index: int = 0):
        """Añade una ciudad al diccionario de ciudades activas."""
        with self._lock:
            self.stats.active_cities[city_name] = {
                'worker_id': worker_id,
                'population': population,
                'index': index,
                'start_time': datetime.now().isoformat(),
                'businesses_total': 0,
                'businesses_processed': 0,
                'status': 'searching',  # searching | scrolling | opening_cards | extracting
                # Campos para fase de búsqueda en Maps
                'scroll_results': 0,  # Resultados encontrados durante scroll
                'cards_total': 0,  # Total de fichas que necesitan abrirse
                'cards_opened': 0,  # Fichas ya abiertas
            }

    def update_city_progress(self, city_name: str, businesses_total: int = None,
                            businesses_processed: int = None, status: str = None,
                            scroll_results: int = None, cards_total: int = None,
                            cards_opened: int = None):
        """Actualiza el progreso de una ciudad activa."""
        with self._lock:
            if city_name in self.stats.active_cities:
                if businesses_total is not None:
                    self.stats.active_cities[city_name]['businesses_total'] = businesses_total
                if businesses_processed is not None:
                    self.stats.active_cities[city_name]['businesses_processed'] = businesses_processed
                if status is not None:
                    self.stats.active_cities[city_name]['status'] = status
                if scroll_results is not None:
                    self.stats.active_cities[city_name]['scroll_results'] = scroll_results
                if cards_total is not None:
                    self.stats.active_cities[city_name]['cards_total'] = cards_total
                if cards_opened is not None:
                    self.stats.active_cities[city_name]['cards_opened'] = cards_opened

    def remove_active_city(self, city_name: str):
        """Elimina una ciudad del diccionario de ciudades activas."""
        with self._lock:
            if city_name in self.stats.active_cities:
                del self.stats.active_cities[city_name]

    def run(self):
        while self._running:
            try:
                with self._lock:
                    # Actualizar runtime (sesión actual + acumulado anterior)
                    self.stats.runtime_seconds = self.base_runtime + (time.time() - self.start_time)

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
