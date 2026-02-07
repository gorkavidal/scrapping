import asyncio
import aiohttp
import re
import csv
import os
import pandas as pd
import math
import logging
from datetime import datetime
from playwright.async_api import async_playwright
import argparse
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from email_validator import validate_email, EmailNotValidError
import tldextract
import json
import hashlib
import curses
import threading
from collections import defaultdict
import time
import signal # Needed for graceful shutdown signal handling
import sys
import subprocess
import uuid

# Importar el sistema de gestión de jobs
from job_manager import (
    JobManager, Job, JobStatus, JobStats,
    ControlThread, StatsThread, ControlCommand,
    format_duration, calculate_eta
)

# Funciones auxiliares de logging
def log_city_info(city_name, population):
    msg = f"{city_name:<30} {population:>8,} habitantes"
    logging.info(msg)

def log_section(msg):
    logging.info("=" * 80)
    logging.info(msg)
    logging.info("=" * 80)

def log_subsection(msg):
    logging.info("-" * 40)
    logging.info(msg)

def log_stats(prefix, value, suffix=""):
    msg = f"{prefix:<40} {value:>8}{suffix}"
    logging.info(msg)

# Estado global para el dashboard
dashboard_state = {
    'active_cities': defaultdict(lambda: {'status': 'Iniciando...', 'progress': '0/0', 'found': 0}),
    'total_processed': 0,
    'total_cities': 0,
    'running': True
}

class DashboardHandler(logging.Handler):
    def __init__(self, dashboard_state):
        super().__init__()
        self.dashboard_state = dashboard_state
        
    def emit(self, record):
        try:
            msg = record.getMessage()
            # Actualizar estado basado en los mensajes de log
            if "=== Procesando ciudad:" in msg:
                city_name = msg.split("'")[1]
                self.dashboard_state['active_cities'][city_name]['status'] = 'Procesando...'
            elif "Encontrados" in msg and "nuevos resultados" in msg:
                city_name = msg.split("para")[1].split(":")[0].strip()
                found = int(msg.split("Encontrados")[1].split("nuevos")[0].strip())
                self.dashboard_state['active_cities'][city_name]['found'] = found
            elif "Total de resultados crudos para" in msg:
                city_name = msg.split("para")[1].split(":")[0].strip()
                total = msg.split(":")[-1].strip()
                self.dashboard_state['active_cities'][city_name]['progress'] = f"{total} encontrados"
        except Exception:
            pass

def update_dashboard(stdscr, dashboard_state):
    while dashboard_state['running']:
        try:
            height, width = stdscr.getmaxyx()
            separator_line = "-" * (width - 2)
            
            # Limpiar la última línea
            stdscr.addstr(height-3, 0, " " * (width-1))
            stdscr.addstr(height-2, 0, " " * (width-1))
            stdscr.addstr(height-1, 0, " " * (width-1))
            
            # Dibujar separador
            stdscr.addstr(height-4, 0, separator_line)
            
            # Mostrar progreso general
            progress = f"Progreso total: {dashboard_state['total_processed']}/{dashboard_state['total_cities']} ciudades"
            stdscr.addstr(height-3, 1, progress)
            
            # Mostrar ciudades activas
            active_info = []
            for city, info in dashboard_state['active_cities'].items():
                if info['status'] != 'Completada':
                    city_info = f"{city}: {info['status']} ({info['progress']}) - Encontrados: {info['found']}"
                    active_info.append(city_info)
            
            if active_info:
                cities_str = " | ".join(active_info[-2:])  # Mostrar solo las últimas 2 ciudades
                stdscr.addstr(height-2, 1, f"Ciudades en proceso: {cities_str}")
            
            stdscr.refresh()
            time.sleep(0.5)
        except curses.error:
            pass  # Ignorar errores de curses (pueden ocurrir al redimensionar la terminal)
        except Exception:
            pass

# Configuración básica del logging con el nuevo handler
def setup_logging(dashboard_state):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            DashboardHandler(dashboard_state)
        ]
    )

# =============================================================================
# CONFIGURACIÓN GLOBAL - Optimizado v2
# =============================================================================

# Concurrencia
MAX_CONCURRENT_EXTRACTIONS = 20  # Extracciones de email en paralelo
CONCURRENT_BROWSERS = 8          # Contextos de browser concurrentes
DEFAULT_WORKERS = 1              # Workers por defecto

# Configuración de límites de búsqueda de emails
MAX_CONTACT_LINKS = 3            # Máx enlaces de contacto por URL principal
MAX_TOTAL_TIME = 20              # Tiempo máx en segundos por URL (reducido de 30)
MAIN_PAGE_TIMEOUT = 8000         # Timeout ms para página principal (reducido de 15000)
CONTACT_PAGE_TIMEOUT = 10000     # Timeout ms para páginas de contacto
NETWORKIDLE_TIMEOUT = 3000       # Timeout ms para networkidle (reducido de 5000)

# Timeouts de scroll optimizados (reducidos de 2000ms a 1000ms)
SCROLL_WAIT_MS = 1000            # Espera entre scrolls (antes 2000)
SCROLL_RETRY_WAIT_MS = 1000      # Espera extra si no hay nuevos resultados (antes 2000)
MAX_NO_NEW_RESULTS = 3           # Intentos sin nuevos resultados antes de parar

# =============================================================================
# RECURSOS COMPARTIDOS GLOBALES (optimización I/O)
# =============================================================================

# Sesión aiohttp compartida - se inicializa en main() y se cierra en finally
_shared_aiohttp_session: aiohttp.ClientSession = None

# Lock global para escritura CSV (evita crear locks nuevos cada vez)
_csv_write_lock = asyncio.Lock()

# Lock global para cache de negocios procesados
_cache_lock = asyncio.Lock()

# Lock global para escritura de checkpoint
_checkpoint_lock = asyncio.Lock()

# Contador de IDs en memoria para evitar leer CSV completo
_csv_id_counters = {
    'with_emails': 0,
    'no_emails': 0
}

# =============================================================================
# CHECKPOINT GRANULAR - Estado de ciudad en progreso
# =============================================================================
# Permite pausar/reanudar a mitad de una ciudad sin perder progreso

# Estado de la ciudad actualmente en proceso
_current_city_state = {
    'city_name': None,              # Nombre de la ciudad en proceso
    'raw_businesses': [],           # Negocios encontrados en Maps (sin procesar emails)
    'processed_indices': set(),     # Índices de negocios ya procesados
    'enriched_results': [],         # Resultados con emails ya extraídos
}

# Lock para actualizar estado de ciudad
_city_state_lock = asyncio.Lock()

async def save_city_checkpoint(checkpoint_file: str, config: dict, processed_cities: set,
                                final_csv: str, no_emails_csv: str, stats_thread=None):
    """Guarda checkpoint incluyendo el estado parcial de la ciudad actual."""
    async with _checkpoint_lock:
        try:
            accumulated_stats = None
            if stats_thread:
                current_stats = stats_thread.get_stats()
                accumulated_stats = {
                    'runtime_seconds': current_stats.runtime_seconds,
                    'results_total': current_stats.results_total,
                    'results_with_email': current_stats.results_with_email,
                    'results_with_corporate_email': current_stats.results_with_corporate_email,
                    'results_with_web': current_stats.results_with_web,
                    'errors_count': current_stats.errors_count,
                    'avg_city_duration': current_stats.avg_city_duration,
                }

            # Preparar estado de ciudad en progreso (si existe)
            city_in_progress = None
            if _current_city_state['city_name']:
                city_in_progress = {
                    'city_name': _current_city_state['city_name'],
                    'raw_businesses': _current_city_state['raw_businesses'],
                    'processed_indices': list(_current_city_state['processed_indices']),
                    'enriched_results': _current_city_state['enriched_results'],
                }

            checkpoint_data = {
                'run_id': config.get('run_id'),
                'processed_cities': list(processed_cities),
                'last_update': datetime.now().isoformat(),
                'final_csv': final_csv,
                'no_emails_csv': no_emails_csv,
                'config': config,
                'accumulated_stats': accumulated_stats,
                'city_in_progress': city_in_progress,  # NUEVO: estado granular
            }
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=4)
        except Exception as e:
            logging.error(f"Error guardando checkpoint: {e}")

def clear_city_state():
    """Limpia el estado de la ciudad actual (cuando se completa)."""
    _current_city_state['city_name'] = None
    _current_city_state['raw_businesses'] = []
    _current_city_state['processed_indices'] = set()
    _current_city_state['enriched_results'] = []

def restore_city_state(city_data: dict):
    """Restaura el estado de una ciudad desde datos del checkpoint."""
    _current_city_state['city_name'] = city_data.get('city_name')
    _current_city_state['raw_businesses'] = city_data.get('raw_businesses', [])
    _current_city_state['processed_indices'] = set(city_data.get('processed_indices', []))
    _current_city_state['enriched_results'] = city_data.get('enriched_results', [])

async def get_shared_session() -> aiohttp.ClientSession:
    """Obtiene la sesión aiohttp compartida, creándola si no existe."""
    global _shared_aiohttp_session
    if _shared_aiohttp_session is None or _shared_aiohttp_session.closed:
        timeout = aiohttp.ClientTimeout(total=30)
        _shared_aiohttp_session = aiohttp.ClientSession(timeout=timeout)
    return _shared_aiohttp_session

async def close_shared_session():
    """Cierra la sesión aiohttp compartida."""
    global _shared_aiohttp_session
    if _shared_aiohttp_session and not _shared_aiohttp_session.closed:
        await _shared_aiohttp_session.close()
        _shared_aiohttp_session = None

# -----------------------------------------------------
# 1. Funciones auxiliares para cargar ciudades (API GeoNames) y obtener su centro
# -----------------------------------------------------

# <<<< GeoNames API Interaction Functions >>>>

async def fetch_geonames_api(endpoint: str, params: dict) -> dict:
    """Función genérica para llamar a la API de GeoNames.

    Usa sesión aiohttp compartida para evitar overhead de conexión.
    """
    base_url = "http://api.geonames.org/"
    url = f"{base_url}{endpoint}"
    logging.debug(f"Llamando a GeoNames API: {url} con params: {params}")
    session = await get_shared_session()
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            # Check content type to decide how to parse
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                data = await response.json()
            elif 'text/xml' in content_type or 'text/html' in content_type:
                # Attempt to parse as JSON first, might work for some errors
                try:
                    data = await response.json()
                except aiohttp.ContentTypeError:
                    # If JSON parsing fails, return raw text for debugging
                    text_data = await response.text()
                    logging.warning(f"GeoNames devolvió tipo {content_type}. Contenido: {text_data[:200]}...")
                    if '{' in text_data and '}' in text_data:
                        try:
                            status_match = re.search(r'"message"\s*:\s*"(.*?)"', text_data)
                            if status_match:
                                return {"status": {"message": status_match.group(1), "value": response.status}}
                        except Exception:
                            pass
                    return {"error": "Formato no JSON inesperado", "status": response.status, "content_type": content_type, "text": text_data[:500]}
            else:
                # Handle unexpected content types
                text_data = await response.text()
                logging.warning(f"GeoNames devolvió tipo inesperado: {content_type}. Contenido: {text_data[:200]}...")
                return {"error": f"Tipo de contenido inesperado: {content_type}", "status": response.status, "text": text_data[:500]}

            # Check for API error messages within the JSON response
            if isinstance(data, dict) and 'status' in data:
                logging.error(f"Error de la API GeoNames: {data['status']['message']} (Valor: {data['status']['value']}) para params: {params}")
                return {"error": data['status']['message'], "status_code": data['status']['value']}

            logging.debug(f"GeoNames API response received for {endpoint}")
            return data
    except aiohttp.ClientResponseError as e:
        logging.error(f"Error HTTP llamando a GeoNames ({url}): {e.status} {e.message}")
        return {"error": f"HTTP Error: {e.status} {e.message}", "status_code": e.status}
    except asyncio.TimeoutError:
        logging.error(f"Timeout llamando a GeoNames API: {url}")
        return {"error": "Timeout"}
    except Exception as e:
        logging.error(f"Error inesperado llamando a GeoNames API ({url}): {e}")
        return {"error": str(e)}

async def get_regions_for_country(country_code: str, geonames_username: str) -> list:
    """Obtiene las divisiones administrativas de primer nivel (regiones/provincias, ADM1) para un país."""
    logging.info(f"Obteniendo divisiones ADM1 para el país: {country_code}")
    params = {
        'country': country_code.upper(),
        'featureCode': 'ADM1', # Administrative division level 1
        'username': geonames_username,
        'style': 'FULL',       # Get more details
        'type': 'JSON'          # Request JSON format
    }
    data = await fetch_geonames_api('searchJSON', params)

    regions = []
    if data and 'geonames' in data:
        regions = sorted([
            {
                'name': region.get('adminName1') or region.get('name'), # Prefer adminName1
                'code': region.get('adminCode1') # Use adminCode1 if needed for filtering cities
            }
            for region in data['geonames']
            # Ensure there is a name and, crucially, an adminCode1 to filter by
            if (region.get('adminName1') or region.get('name')) and region.get('adminCode1')
        ], key=lambda x: x['name'])
        logging.info(f"Encontradas {len(regions)} divisiones ADM1 con código para {country_code}.")
    elif isinstance(data, dict) and "error" in data:
         # Only print if it's a dictionary with an error key
         print(f"\nError al obtener divisiones ADM1: {data['error']} (Asegúrate que tu usuario '{geonames_username}' está activo y tiene créditos)")
    else:
        logging.warning(f"No se encontraron divisiones ADM1 con código para {country_code}. Respuesta: {data}")

    return regions

# New function to get ADM2 divisions
async def get_admin_level_2_divisions(country_code: str, adm1_code: str, geonames_username: str) -> list:
    """Obtiene las divisiones administrativas de segundo nivel (provincias/condados, ADM2) para una división ADM1 específica."""
    if not adm1_code:
        logging.warning("Se requiere código ADM1 para obtener divisiones ADM2.")
        return []
    logging.info(f"Obteniendo divisiones ADM2 para {country_code}, ADM1: {adm1_code}")
    params = {
        'country': country_code.upper(),
        'adminCode1': adm1_code,
        'featureCode': 'ADM2', # Administrative division level 2
        'username': geonames_username,
        'style': 'FULL',
        'type': 'JSON'
    }
    data = await fetch_geonames_api('searchJSON', params)

    subdivisions = []
    if data and 'geonames' in data:
        subdivisions = sorted([
            {
                'name': sub.get('adminName2') or sub.get('name'), # Prefer adminName2
                'code': sub.get('adminCode2') # Use adminCode2 for filtering cities
            }
            for sub in data['geonames']
            # Ensure name and, crucially, adminCode2
            if (sub.get('adminName2') or sub.get('name')) and sub.get('adminCode2')
        ], key=lambda x: x['name'])
        logging.info(f"Encontradas {len(subdivisions)} divisiones ADM2 con código para ADM1 {adm1_code}.")
    elif isinstance(data, dict) and "error" in data:
        print(f"\nError al obtener divisiones ADM2: {data['error']}")
    else:
        logging.warning(f"No se encontraron divisiones ADM2 con código para ADM1 {adm1_code}. Respuesta: {data}")

    return subdivisions


# Modified function to handle population range and ADM2 code
async def fetch_cities_geonames(country_code: str, geonames_username: str, min_population: int, max_population: int = None, region_code: str = None, adm2_code: str = None) -> list:
    """Obtiene ciudades de GeoNames filtrando por país, población (min/max), y opcionalmente región ADM1 y/o subdivisión ADM2.

    Implementa paginación si se busca en todo el país (sin ADM1/ADM2) Y se especifica una población máxima.
    """
    population_log = f"{min_population:,}"
    if max_population is not None:
        population_log += f" - {max_population:,}"
    else:
        population_log += " o más"

    logging.info(f"Obteniendo ciudades de GeoNames para {country_code} (Pob: {population_log}), ADM1: {region_code or 'Todas'}, ADM2: {adm2_code or 'Todas'}")

    # Determine if pagination is needed: Activate if max_population is set.
    use_pagination = max_population is not None
    page_size = 1000 # Max results per page
    # Limit pagination to 5 pages (5000 results) for GeoNames free tier
    max_pages = 5

    params = {
        'country': country_code.upper(),
        'username': geonames_username,
        'style': 'FULL',
        'type': 'JSON',
        'orderby': 'population', # Order by population descending
        'population_gte': f'{min_population}',
    }

    # Add ADM filters BEFORE deciding on pagination, so they apply in both cases
    if region_code:
        params['adminCode1'] = region_code
    if adm2_code:
        params['adminCode2'] = adm2_code

    all_geonames_results = []
    fetch_error = None

    if use_pagination:
        logging.info("Población máxima especificada: Iniciando paginación (máx 5 páginas)...")
        # Set feature filter based on whether it's a regional or national paginated search
        if region_code is None and adm2_code is None:
            # National paginated search: Use PPL to try and get smaller places earlier
            params['featureCode'] = 'PPL'
            logging.info(" -> Usando featureCode=PPL para paginación nacional.")
        else:
            # Regional paginated search: Use standard FeatureClass P
            params['featureClass'] = 'P'
            logging.info(f" -> Usando featureClass=P para paginación regional (ADM1: {region_code}, ADM2: {adm2_code}).")

        params['maxRows'] = page_size # Set page size for pagination
        start_row = 0

        while start_row < max_pages * page_size:
            logging.info(f"Paginación: Obteniendo página {start_row // page_size + 1} (desde fila {start_row})...")
            params['startRow'] = start_row
            data = await fetch_geonames_api('searchJSON', params)

            if not data or 'geonames' not in data or isinstance(data, dict) and "error" in data:
                if isinstance(data, dict) and "error" in data:
                    fetch_error = data['error']
                    print(f"\\nError de API GeoNames durante paginación: {fetch_error}")
                else:
                     logging.warning(f"Paginación: Respuesta inesperada o vacía en página {start_row // page_size + 1}. Respuesta: {data}")
                break # Stop pagination on error or empty response

            current_page_results = data['geonames']
            num_results_in_page = len(current_page_results)
            logging.info(f"Paginación: Página {start_row // page_size + 1} devolvió {num_results_in_page} resultados.")

            if num_results_in_page == 0:
                logging.info("Paginación: No hay más resultados.")
                break # No more results

            all_geonames_results.extend(current_page_results)
            start_row += num_results_in_page

            # If fewer results than page size returned, assume it's the last page
            if num_results_in_page < page_size:
                logging.info("Paginación: Última página alcanzada.")
                break

            # Brief pause between pages
            await asyncio.sleep(0.5)

        if start_row >= max_pages * page_size:
            logging.warning(f"Paginación: Límite de {max_pages} páginas alcanzado. Puede haber más resultados no obtenidos.")
        logging.info(f"Paginación completada. Total resultados brutos obtenidos: {len(all_geonames_results)}")

    else:
        # Standard fetch (no pagination)
        logging.info("Realizando búsqueda estándar (filtrada por región/ADM2 o sin población máx.).")
        params['featureClass'] = 'P' # Use broader feature class for standard search
        params['maxRows'] = page_size # Still use maxRows=1000

        data = await fetch_geonames_api('searchJSON', params)

        if data and 'geonames' in data:
            all_geonames_results = data['geonames']
        elif isinstance(data, dict) and "error" in data:
            fetch_error = data['error']
            print(f"\\nError de API GeoNames: {fetch_error}")
        else:
            logging.warning(f"No se obtuvieron resultados o hubo respuesta inesperada. Respuesta: {data}")

    # --- Local Filtering (applies to both paginated and standard results) --- #
    filtered_cities = []
    if not all_geonames_results:
         if fetch_error:
             # Error already printed
             pass
         else:
             logging.warning("No se encontraron ciudades en GeoNames que cumplan los criterios iniciales.")
         return [] # Return empty list if no raw results

    logging.info(f"Filtrando localmente {len(all_geonames_results)} resultados brutos...")
    processed_count = 0
    skipped_min_pop = 0
    skipped_max_pop = 0
    skipped_adm1 = 0
    skipped_adm2 = 0

    for place in all_geonames_results:
        processed_count += 1
        # Ensure it has population data
        population = place.get('population', 0)

        # Filter locally by min_population (double check)
        if population < min_population:
            skipped_min_pop += 1
            logging.debug(f"SKIP (min_pop): '{place.get('name', 'N/A')}' ({population:,}) < {min_population:,}")
            continue
        # Filter by max_population (if specified)
        if max_population is not None and population > max_population:
            skipped_max_pop += 1
            logging.debug(f"SKIP (max_pop): '{place.get('name', 'N/A')}' ({population:,}) > {max_population:,}")
            continue # Skip if population exceeds maximum

        # Check if ADM codes match if they were provided (API filter isn't always perfect)
        # This check is more relevant for the non-paginated case
        if region_code and place.get('adminCode1') != region_code:
            skipped_adm1 += 1
            logging.debug(f"SKIP (ADM1): '{place.get('name', 'N/A')}' ({place.get('adminCode1')} != {region_code})")
            continue
        if adm2_code and place.get('adminCode2') != adm2_code:
            skipped_adm2 += 1
            logging.debug(f"SKIP (ADM2): '{place.get('name', 'N/A')}' ({place.get('adminCode2')} != {adm2_code})")
            continue

        # Add city if all checks pass
        filtered_cities.append({
            'name': place['name'],
            'population': population,
            'country_code': place.get('countryCode'),
            'geonameId': place.get('geonameId'),
            'lat': place.get('lat'), # Get lat/lon directly if available
            'lon': place.get('lng'), # Note: API uses 'lng'
            'adminName1': place.get('adminName1', ''), # Region/Province name (ADM1)
            'adminCode1': place.get('adminCode1', ''), # ADM1 code
            'adminName2': place.get('adminName2', ''), # Subdivision name (ADM2)
            'adminCode2': place.get('adminCode2', '')  # ADM2 code
        })

    logging.info(f"Filtrado local completado: {processed_count} procesados, {skipped_min_pop} < min_pop, {skipped_max_pop} > max_pop, {skipped_adm1} ADM1 mismatch, {skipped_adm2} ADM2 mismatch.")
    logging.info(f"Encontradas {len(filtered_cities)} ciudades que cumplen TODOS los criterios finales.")

    # Sort by population descending locally as well
    filtered_cities.sort(key=lambda x: x['population'], reverse=True)

    return filtered_cities

async def get_city_location(city_name: str, country_code: str, city_data: dict = None) -> dict:
    """
    Obtiene la ubicación (latitud y longitud) del centro de la ciudad.
    Prioritiza lat/lon de GeoNames si está disponible, si no, usa Nominatim.
    """
    # Prioritize GeoNames coordinates if available in city_data
    if city_data and city_data.get('lat') and city_data.get('lon'):
        try:
            lat = float(city_data['lat'])
            lon = float(city_data['lon'])
            logging.info(f"Usando ubicación de GeoNames para {city_name}: ({lat}, {lon})")
            return {"center_lat": lat, "center_lon": lon}
        except (ValueError, TypeError):
             logging.warning(f"Coordenadas inválidas de GeoNames para {city_name}. Usando Nominatim.")
        
    # Fallback to Nominatim if GeoNames coords are missing or invalid
    logging.info(f"Obteniendo ubicación para {city_name}, {country_code} desde Nominatim (fallback)")
    session = await get_shared_session()
    url = f"https://nominatim.openstreetmap.org/search?q={quote_plus(city_name)}&countrycodes={country_code.lower()}&format=json&limit=1"
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                logging.warning(f"Nominatim devolvió estado {response.status} para {city_name}, {country_code}")
                # Fallback query without country code
                url = f"https://nominatim.openstreetmap.org/search?q={quote_plus(f'{city_name}')}&format=json&limit=1"
                async with session.get(url, headers=headers) as fallback_response:
                    if fallback_response.status == 200:
                        data = await fallback_response.json()
                    else:
                        logging.error(f"Error en fallback de Nominatim para {city_name}: Estado {fallback_response.status}")
                        data = []
            else:
                data = await response.json()

            if data:
                center_lat = float(data[0]["lat"])
                center_lon = float(data[0]["lon"])
                logging.info(f"Ubicación de Nominatim para {city_name}: ({center_lat}, {center_lon})")
                return {"center_lat": center_lat, "center_lon": center_lon}
            else:
                logging.warning(f"No se encontró ubicación en Nominatim para {city_name}, {country_code}")
    except aiohttp.ClientError as e:
        logging.error(f"Error de red (aiohttp) al obtener ubicación de Nominatim para {city_name}: {e}")
    except asyncio.TimeoutError:
        logging.error(f"Timeout al obtener ubicación de Nominatim para {city_name}")
    except Exception as e:
        logging.error(f"Error inesperado al obtener ubicación de Nominatim para {city_name}: {e}", exc_info=True)
    return None

# -----------------------------------------------------
# 2. Función para generar centros de celda en función del tamaño en metros
# -----------------------------------------------------

def generate_grid_cells(city_center: dict, grid_size: int, cell_size_m: float, strategy: str = "simple") -> list:
    """
    Genera una lista de diccionarios, cada uno con el centro (lat, lon) de una celda.
    Soporta dos estrategias:
    - "simple": Una sola celda con tamaño variable según población
    - "grid": División en cuadrícula según población
    """
    center_lat = city_center["center_lat"]
    center_lon = city_center["center_lon"]
    delta_lat = cell_size_m / 111000
    delta_lon = cell_size_m / (111000 * math.cos(math.radians(center_lat)))
    cells = []
    
    if strategy == "simple" or grid_size == 1:
        cells.append({"center_lat": center_lat, "center_lon": center_lon})
    else:
        for i in range(grid_size):
            row_center = center_lat + ((grid_size/2 - i - 0.5) * delta_lat)
            for j in range(grid_size):
                col_center = center_lon + ((j + 0.5 - grid_size/2) * delta_lon)
                cells.append({
                    "center_lat": row_center,
                    "center_lon": col_center
                })
    
    logging.info(f"Generados {len(cells)} centros de celda con estrategia {strategy}")
    return cells

# -----------------------------------------------------
# 3. Funciones auxiliares de extracción de emails
# -----------------------------------------------------

async def extract_emails_from_url(url: str) -> set:
    """Extrae emails de una URL usando la sesión aiohttp compartida."""
    emails_found = set()
    try:
        session = await get_shared_session()
        async with session.get(url) as response:
            await asyncio.sleep(0.5)
            html = await response.text()
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails_found.update(re.findall(email_pattern, html))
    except Exception as e:
        logging.debug(f"Error al extraer emails de {url}: {e}")
    return emails_found


# =============================================================================
# EXTRACCIÓN HTTP-FIRST (Optimización v3)
# =============================================================================
# Intenta primero con HTTP puro (rápido), solo usa Playwright como fallback

async def _extract_emails_http(url: str, start_time: float) -> dict:
    """Extrae emails usando HTTP puro (sin navegador).

    Más rápido que Playwright (~100ms vs ~2-5s).
    Funciona para la mayoría de sitios web estáticos.

    Returns:
        dict con:
        - 'emails' (set): emails encontrados
        - 'page_loaded' (bool): True si la página se cargó y tiene contenido real
        - 'is_spa' (bool): True si detectamos que es una SPA que necesita JS
        - 'error' (str): mensaje de error si hubo
    """
    emails_found = set()
    page_loaded = False
    is_spa = False
    error_message = ""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    try:
        session = await get_shared_session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        }

        async with session.get(url, headers=headers, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status != 200:
                error_message = f"HTTP {response.status}"
                return {'emails': emails_found, 'page_loaded': False, 'is_spa': False, 'error': error_message}

            html = await response.text()
            html_lower = html.lower()

            # Detectar si es una SPA (Single Page App) que NECESITA JavaScript
            # Criterio MUY estricto: solo marcar como SPA si:
            # 1. Casi no hay texto visible (<100 chars)
            # 2. Hay indicadores EXPLÍCITOS de que requiere JS
            # (La mayoría de webs con poco contenido simplemente no tienen emails)

            soup = BeautifulSoup(html, 'html.parser')
            # Eliminar scripts y styles para contar texto real
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            visible_text = soup.get_text(separator=' ', strip=True)

            # Indicadores EXPLÍCITOS de que la página requiere JavaScript
            js_required_indicators = [
                'please enable javascript',
                'javascript is required',
                'you need to enable javascript',
                'this site requires javascript',
                'enable javascript to view',
                'javascript must be enabled',
            ]

            # Solo es SPA si: casi vacía (<100 chars) Y mensaje explícito de JS requerido
            has_js_required_msg = any(ind in html_lower for ind in js_required_indicators)
            is_spa = len(visible_text) < 100 and has_js_required_msg

            # La página se cargó correctamente si hay contenido real
            page_loaded = len(visible_text) > 100

            # Extraer emails del contenido
            found = re.findall(email_pattern, html)
            emails_found.update(found)

            # Buscar enlaces de contacto en el HTML (solo si la página cargó)
            if page_loaded and time.time() - start_time < MAX_TOTAL_TIME:
                contact_keywords = ['contact', 'kontakt', 'contacto', 'contacta', 'about', 'sobre', 'nosotros', 'empresa']
                contact_links = []

                for link in soup.find_all('a', href=True):
                    href = link.get('href', '').lower()
                    text = link.get_text().lower()
                    if any(kw in href or kw in text for kw in contact_keywords):
                        original_href = link.get('href', '')
                        if original_href.startswith('/'):
                            from urllib.parse import urljoin
                            full_url = urljoin(url, original_href)
                        elif original_href.startswith('http'):
                            full_url = original_href
                        else:
                            continue
                        if full_url not in contact_links:
                            contact_links.append(full_url)

                # Visitar páginas de contacto (máximo 2 para HTTP)
                for contact_url in contact_links[:2]:
                    if time.time() - start_time > MAX_TOTAL_TIME:
                        break
                    try:
                        async with session.get(contact_url, headers=headers, allow_redirects=True,
                                              timeout=aiohttp.ClientTimeout(total=8)) as contact_resp:
                            if contact_resp.status == 200:
                                contact_html = await contact_resp.text()
                                contact_emails = re.findall(email_pattern, contact_html)
                                emails_found.update(contact_emails)
                    except Exception:
                        continue

    except asyncio.TimeoutError:
        error_message = "Timeout"
    except aiohttp.ClientError as e:
        error_message = f"HTTP: {type(e).__name__}"
    except Exception as e:
        error_message = f"Error: {type(e).__name__}"

    return {'emails': emails_found, 'page_loaded': page_loaded, 'is_spa': is_spa, 'error': error_message}


async def _extract_emails_with_browser(browser, url: str, start_time: float,
                                       emails_found: set, create_own_browser: bool = False) -> dict:
    """Función auxiliar para extraer emails usando un browser Playwright.

    Args:
        browser: Instancia de browser Playwright
        url: URL a procesar
        start_time: Tiempo de inicio para controlar timeout
        emails_found: Set para acumular emails encontrados
        create_own_browser: Si True, cierra el browser al terminar

    Returns:
        dict con 'emails' (set) y 'error' (str)
    """
    error_message = ""
    context = None
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    try:
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        page = await context.new_page()

        try:
            await page.goto(url, timeout=MAIN_PAGE_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)

            # Extraer emails del contenido HTML
            content = await page.content()
            found_emails = re.findall(email_pattern, content)
            emails_found.update(found_emails)

            # Buscar enlaces de contacto
            contact_links = await page.evaluate('''
                () => {
                    const links = Array.from(document.querySelectorAll('a'));
                    return links
                        .filter(link => {
                            const text = (link.textContent || '').toLowerCase();
                            const href = (link.href || '').toLowerCase();
                            return ['contact', 'kontakt', 'contacto', 'contacta', 'about', 'sobre', 'nosotros', 'empresa']
                                .some(keyword => text.includes(keyword) || href.includes(keyword));
                        })
                        .map(link => link.href)
                        .filter(href => href.startsWith('http'));
                }
            ''')

            # Limitar enlaces de contacto
            contact_links = contact_links[:MAX_CONTACT_LINKS]

            # Extraer emails de páginas de contacto
            for contact_url in contact_links:
                if time.time() - start_time > MAX_TOTAL_TIME:
                    break
                try:
                    await page.goto(contact_url, timeout=CONTACT_PAGE_TIMEOUT)
                    await page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)
                    contact_content = await page.content()
                    contact_emails = re.findall(email_pattern, contact_content)
                    emails_found.update(contact_emails)
                except Exception:
                    continue

        except Exception:
            error_message = "Error"

    finally:
        if context:
            await context.close()
        if create_own_browser and browser:
            await browser.close()

    return {'emails': emails_found, 'error': error_message}


async def extract_contact_info(url, business_name="", extraction_semaphore: asyncio.Semaphore = None,
                               current_index=0, total_urls=0, browser=None) -> dict:
    """Extrae información de contacto (emails) de una URL.

    OPTIMIZACIÓN v3: HTTP-first con fallback a Playwright.
    - Primero intenta HTTP puro (~100ms) - funciona para ~80% de webs
    - Solo usa Playwright si HTTP falla o no encuentra emails en sitio JS

    Args:
        url: URL del negocio a procesar
        business_name: Nombre del negocio (para logs)
        extraction_semaphore: Semáforo para limitar concurrencia
        current_index: Índice actual (para logs)
        total_urls: Total de URLs (para logs)
        browser: Browser Playwright compartido (para fallback JS)
    """
    if not url:
        return {"Email_Raw": "", "Email_Filtered": "", "Email_Search_Status": "No URL proporcionada"}

    if extraction_semaphore is None:
        extraction_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)

    emails_found = set()
    error_message = ""
    method_used = "HTTP"
    start_time = time.time()

    try:
        async with extraction_semaphore:
            # PASO 1: Intentar HTTP primero (rápido, ~100ms)
            http_result = await _extract_emails_http(url, start_time)
            emails_found = http_result['emails']

            # PASO 2: Fallback a Playwright SOLO si:
            # - La página es una SPA que NO renderizó contenido (is_spa=True)
            # - O hubo un error de conexión/timeout
            # NO hacemos fallback solo porque no encontró emails (puede que no tenga)
            needs_playwright = (
                http_result['is_spa']  # Es SPA vacía, necesita JS para renderizar
                or (http_result['error'] and not http_result['page_loaded'])  # Error y no cargó
            )

            if needs_playwright and browser is not None:
                method_used = "Playwright"
                logging.debug(f"[{current_index}/{total_urls}] Fallback a Playwright (SPA/error) para {url}")
                result = await _extract_emails_with_browser(
                    browser, url, start_time, emails_found, create_own_browser=False
                )
                emails_found = result['emails']
                if result['error']:
                    error_message = result['error']
            elif http_result['error']:
                error_message = http_result['error']

    except Exception as e:
        error_message = f"Error: {type(e).__name__}"

    elapsed = time.time() - start_time
    status_suffix = f" (SPA)" if method_used == "Playwright" else ""
    logging.info(f"[{current_index}/{total_urls}] {method_used}{status_suffix} {url} ({elapsed:.1f}s) -> {len(emails_found)} emails")
    
    # Filtrar y validar emails
    valid_emails = []
    for email in emails_found:
        try:
            valid = validate_email(email, check_deliverability=False).email
            valid_emails.append(valid)
        except EmailNotValidError:
            continue
    
    filtered_emails = [
        email for email in valid_emails
        if not re.search(r'@.*2x\.(png|jpg|jpeg)$', email, re.IGNORECASE)
    ]
    
    email_raw = "; ".join(filtered_emails) if filtered_emails else ""
    
    # Extraer el dominio base de la URL
    business_domain_base = ""
    try:
        extracted = tldextract.extract(url)
        if extracted.domain:
            business_domain_base = extracted.domain.lower()
    except:
        pass
    
    # Priorizar emails corporativos
    corporate_emails = [
        email for email in filtered_emails
        if business_domain_base and email.split('@')[-1].split('.')[0].lower() == business_domain_base
    ]
    
    email_filtered = "; ".join(corporate_emails) if corporate_emails else ""
    
    # Determinar el estado de la búsqueda
    if error_message:
        status = "Error"
    elif not email_filtered and not email_raw:
        status = "Sin emails"
    elif not email_filtered and email_raw:
        status = "Solo emails no corporativos"
    else:
        status = "Emails corporativos encontrados"
    
    logging.info(f"[{current_index}/{total_urls}] Resultado para {url}: {status}")
    
    return {
        "Email_Raw": email_raw,
        "Email_Filtered": email_filtered,
        "Email_Search_Status": status
    }

# -----------------------------------------------------
# 4. Scraping de Google Maps en una celda (cuadrante)
# -----------------------------------------------------

def normalize_phone(phone: str) -> str:
    """Normaliza un número de teléfono eliminando espacios y caracteres especiales."""
    if not phone:
        return ""
    # Eliminar todos los caracteres no numéricos
    return re.sub(r'[^0-9+]', '', phone)

def normalize_address(address: str) -> str:
    """Normaliza una dirección eliminando espacios extra y caracteres especiales."""
    if not address:
        return ""
    # Convertir a minúsculas
    address = address.lower()
    # Eliminar caracteres especiales y espacios múltiples
    address = re.sub(r'[^\w\s]', ' ', address)
    address = re.sub(r'\s+', ' ', address)
    # Eliminar palabras comunes que pueden variar
    common_words = ['calle', 'avenida', 'plaza', 'street', 'avenue', 'road', 'st', 'ave', 'rd']
    for word in common_words:
        address = re.sub(fr'\b{word}\b', '', address)
    return address.strip()

def normalize_name(name: str) -> str:
    """Normaliza un nombre de negocio eliminando caracteres especiales y palabras comunes."""
    if not name:
        return ""
    # Convertir a minúsculas
    name = name.lower()
    # Eliminar caracteres especiales y espacios múltiples
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    # Eliminar palabras comunes que pueden variar
    common_words = ['ltd', 'limited', 'inc', 'incorporated', 'sl', 'sa', 'slu', 'sl.', 's.l.', 's.a.']
    for word in common_words:
        name = re.sub(fr'\b{word}\b', '', name)
    return name.strip()

def get_business_id(business: dict) -> str:
    """
    Genera un identificador único para un negocio basado en sus datos normalizados.
    La clave se genera usando una combinación de:
    1. Nombre normalizado (sin caracteres especiales ni palabras comunes)
    2. Dirección normalizada (sin palabras comunes como 'calle', 'avenida', etc.)
    3. Teléfono normalizado (solo números)
    4. Código postal extraído de la dirección (si existe)
    """
    name = normalize_name(business.get("Name", ""))
    address = normalize_address(business.get("Address", ""))
    phone = normalize_phone(business.get("Phone", ""))
    
    # Intentar extraer código postal (formato UK: AA1 1AA o ES: 28001)
    postcode = ""
    uk_postcode = re.search(r'[A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2}', business.get("Address", ""))
    es_postcode = re.search(r'\b\d{5}\b', business.get("Address", ""))
    
    if uk_postcode:
        postcode = uk_postcode.group().replace(" ", "")
    elif es_postcode:
        postcode = es_postcode.group()
    
    # Crear componentes de la clave
    key_components = []
    
    # El nombre es obligatorio
    if not name:
        return ""
    key_components.append(name)
    
    # Añadir otros componentes si existen
    if postcode:
        key_components.append(postcode)
    if phone:
        key_components.append(phone)
    if address and not postcode:  # Si no hay código postal, usar la dirección completa
        key_components.append(address)
    
    # Generar hash de la combinación de componentes
    key = "|".join(key_components)
    return hashlib.md5(key.encode()).hexdigest()

async def scrape_google_maps_cell(query: str, cell_center: dict, fixed_zoom: int, max_results: int, browser, semaphore: asyncio.Semaphore, visited_places: set = None, control_thread=None, city_name: str = "", stats_thread=None) -> tuple:
    """
    Extrae negocios de una celda de Google Maps.

    Returns:
        tuple: (results: list, was_interrupted: bool)
    """
    if visited_places is None:
        visited_places = set()

    # Crear un conjunto local para esta celda
    local_visited = set()

    # Helper para verificar si hay que parar inmediatamente
    def should_stop_immediately() -> bool:
        if control_thread is None:
            return False
        # STOP global afecta a todos los workers
        if control_thread.stop_flag:
            return True
        # RETRY solo afecta a esta ciudad específica
        if control_thread.retry_city == city_name:
            return True
        return False

    # Helper para actualizar progreso de búsqueda
    def update_search_progress(status: str = None, scroll_results: int = None,
                               cards_total: int = None, cards_opened: int = None):
        if stats_thread and city_name:
            stats_thread.update_city_progress(
                city_name=city_name,
                status=status,
                scroll_results=scroll_results,
                cards_total=cards_total,
                cards_opened=cards_opened
            )
    
    lat = cell_center["center_lat"]
    lon = cell_center["center_lon"]
    safe_chars = '"()'
    query_encoded = quote_plus(query, safe=safe_chars)
    url = f"https://www.google.com/maps/search/{query_encoded}/@{lat},{lon},{fixed_zoom}z"
    logging.info(f"[Celda] Navegando a: {url}")
    
    async with semaphore:
        try:
            # Cargar cookies guardadas si existen (del --setup)
            storage_state_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser_data", "google_maps_state.json")
            has_storage_state = os.path.exists(storage_state_file)
            context_kwargs = {
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'viewport': {'width': 1920, 'height': 1080}
            }
            if has_storage_state:
                context_kwargs['storage_state'] = storage_state_file
            context = await browser.new_context(**context_kwargs)
            page = await context.new_page()
            response = await page.goto(url)
            logging.info(f"[Celda] Status code: {response.status}")
            await page.wait_for_load_state("domcontentloaded")
            # Esperar a que aparezca el feed de resultados o timeout razonable
            try:
                await page.wait_for_selector(".m6QErb[role='feed']", timeout=10000)
                logging.info("[Celda] Feed de resultados detectado.")
            except Exception:
                logging.debug("[Celda] Feed no encontrado tras 10s, continuando igualmente...")
            await page.wait_for_timeout(2000)

            # OPTIMIZACIÓN v2: Solo intentar aceptar cookies si NO hay storage_state guardado
            # Esto ahorra ~4 segundos por celda (8 selectores × 500ms timeout)
            if not has_storage_state:
                cookie_selectors = [
                    "button:has-text('Accept all')",
                    "[aria-label='Accept all']",
                    "button:has-text('Aceptar todo')",
                    "[aria-label='Aceptar todo']",
                    "#L2AGLb",
                    "button:has-text('I agree')",
                    "[aria-label='I agree']",
                    "button:has-text('Agree')",
                    "[aria-label='Agree']"
                ]
                for selector in cookie_selectors:
                    try:
                        cookie_button = await page.wait_for_selector(selector, timeout=500)
                        if cookie_button:
                            await cookie_button.click()
                            logging.info("[Celda] Cookies aceptadas.")
                            await page.wait_for_timeout(100)
                            break
                    except Exception as e:
                        logging.debug(f"[Celda] Selector {selector} no funcionó: {e}")
                        continue

            await page.wait_for_timeout(1000)

            # === SCROLL INCREMENTAL CON VERIFICACIÓN DE INTERRUPCIÓN ===
            # En lugar de un script JS monolítico, hacemos scrolls desde Python
            # para poder verificar interrupción entre cada uno
            last_height = 0
            no_new_results = 0
            scroll_interrupted = False

            # Actualizar estado a "scrolling"
            update_search_progress(status='scrolling', scroll_results=0)

            while no_new_results < MAX_NO_NEW_RESULTS:
                # === VERIFICAR INTERRUPCIÓN ANTES DE CADA SCROLL ===
                if should_stop_immediately():
                    logging.info(f"[Celda] Interrupción solicitada durante scroll para {city_name}")
                    scroll_interrupted = True
                    break

                # Ejecutar un único scroll y obtener estado + conteo de resultados
                # Timeout de 5s para evitar bloqueos indefinidos
                try:
                    scroll_result = await asyncio.wait_for(
                        page.evaluate('''
                            () => {
                                const container = document.querySelector(".m6QErb[role='feed']");
                                if (!container) return { height: 0, reachedEnd: true, count: 0 };
                                container.scrollTo(0, container.scrollHeight);
                                const reachedEnd = !!container.querySelector('.HlvSq');
                                const cards = document.querySelectorAll('.Nv2PK');
                                return { height: container.scrollHeight, reachedEnd: reachedEnd, count: cards.length };
                            }
                        '''),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    logging.warning(f"[Celda] Timeout en scroll para {city_name}, verificando stop...")
                    if should_stop_immediately():
                        scroll_interrupted = True
                        break
                    # Continuar con valores por defecto
                    scroll_result = {'height': last_height, 'reachedEnd': False, 'count': 0}

                current_height = scroll_result.get('height', 0)
                reached_end = scroll_result.get('reachedEnd', False)
                current_count = scroll_result.get('count', 0)

                # Actualizar progreso con el conteo actual
                update_search_progress(scroll_results=current_count)

                if reached_end:
                    logging.debug(f"[Celda] Llegamos al final del feed ({current_count} resultados)")
                    break

                if current_height == last_height:
                    no_new_results += 1
                else:
                    no_new_results = 0
                    last_height = current_height

                # Esperar entre scrolls usando asyncio.sleep para poder ser cancelado
                await asyncio.sleep(SCROLL_WAIT_MS / 1000.0)

            if scroll_interrupted:
                await context.close()
                return ([], True)

            logging.info(f"[Celda] Scroll finalizado con altura: {last_height}")

            # Extracción inicial de datos
            initial_results = await page.evaluate('''
                () => {
                    function isValidAddress(txt) {
                        if (/^\\d+(,\\d+)?(\\s*\\(\\d+\\))?$/.test(txt)) return false;
                        return true;
                    }
                    const data = [];
                    const cards = document.querySelectorAll('.Nv2PK');
                    cards.forEach(card => {
                        const nameElem = card.querySelector('.qBF1Pd');
                        const name = nameElem ? nameElem.innerText.trim() : "";
                        const phoneElem = card.querySelector('.UsdlK');
                        const phone = phoneElem ? phoneElem.innerText.trim() : "";
                        const ratingElem = card.querySelector('.MW4etd');
                        const rating = ratingElem ? ratingElem.innerText.trim() : "";
                        let possibleAddresses = [];
                        const infoElems = card.querySelectorAll('.W4Efsd');
                        infoElems.forEach(elem => {
                            const txt = elem.innerText.trim();
                            if (txt.length > 5 && /\\d/.test(txt) && isValidAddress(txt)) {
                                possibleAddresses.push(txt);
                            }
                        });
                        const address = possibleAddresses.length > 0 ? possibleAddresses[0] : "";
                        
                        // Buscar el sitio web en el listado principal (múltiples estrategias)
                        let website = "";

                        // 1. Buscar por data-item-id="authority" (selector más fiable)
                        const authorityLink = card.querySelector('a[data-item-id="authority"]');
                        if (authorityLink) {
                            website = authorityLink.href;
                        }

                        // 2. Buscar por data-value
                        if (!website) {
                            const websiteLink = card.querySelector('a[data-value="Website"], a[data-value="Sitio web"]');
                            if (websiteLink) {
                                website = websiteLink.href;
                            }
                        }

                        // 3. Buscar links externos (no google.com/maps)
                        if (!website) {
                            const allLinks = card.querySelectorAll('a[href]');
                            allLinks.forEach(link => {
                                if (website) return; // Ya encontrado
                                const href = link.href || '';
                                // Excluir links de Google Maps
                                if (href && !href.includes('google.com/maps') &&
                                    !href.startsWith('javascript:') &&
                                    (href.startsWith('http://') || href.startsWith('https://'))) {
                                    // Verificar que no es un link interno de Google
                                    if (!href.includes('google.com') && !href.includes('goo.gl')) {
                                        website = href;
                                    }
                                }
                            });
                        }
                        
                        // Solo guardar el PlaceUrl si no encontramos el sitio web
                        let placeUrl = "";
                        if (!website) {
                            const cardLink = card.querySelector('a.hfpxzc');
                            placeUrl = cardLink ? cardLink.href : "";
                        }
                        
                        // Solo añadir si tenemos al menos nombre y dirección o teléfono
                        if (name && (address || phone)) {
                            data.push({ 
                                "Name": name, 
                                "Phone": phone, 
                                "Rating": rating, 
                                "Address": address, 
                                "Website": website,
                                "PlaceUrl": placeUrl
                            });
                        }
                    });
                    return data;
                }
            ''')
            
            # Estadísticas de extracción
            with_website = sum(1 for r in initial_results if r.get("Website"))
            without_website = len(initial_results) - with_website
            logging.info(f"Número de resultados en el listado: {len(initial_results)} ({with_website} con web directa, {without_website} requieren ficha)")

            # Actualizar estado: ahora abriendo fichas
            if without_website > 0:
                update_search_progress(status='opening_cards', cards_total=without_website, cards_opened=0)

            # Procesar cada resultado y abrir la ficha solo si es necesario
            final_results = []
            was_interrupted = False
            cards_opened_count = 0

            # === TIMEOUT ADAPTATIVO PARA FICHAS ===
            # Empezamos con timeout bajo y aumentamos si hay fallos por timeout
            CARD_TIMEOUTS = [1500, 2500, 4000, 5000]  # Escalera de timeouts en ms
            current_timeout_idx = 0  # Empezamos con el más bajo
            consecutive_successes = 0  # Éxitos consecutivos para bajar timeout
            consecutive_timeouts = 0  # Timeouts consecutivos para subir

            for result in initial_results:
                # === VERIFICACIÓN DE INTERRUPCIÓN EN CADA FICHA ===
                if should_stop_immediately():
                    logging.info(f"[Celda] Interrupción solicitada durante procesamiento de fichas para {city_name}")
                    was_interrupted = True
                    break

                # Crear una clave única para este lugar
                business_id = get_business_id(result)

                # Si no se pudo generar un ID válido o ya está visitado, saltarlo
                if not business_id or business_id in local_visited or business_id in visited_places:
                    continue

                # Marcar como visitado tanto localmente como globalmente
                local_visited.add(business_id)
                visited_places.add(business_id)

                if not result.get("Website") and result.get("PlaceUrl"):
                    card_timeout = CARD_TIMEOUTS[current_timeout_idx]

                    try:
                        logging.debug(f"[Celda] Abriendo ficha para: {result['Name']} (timeout={card_timeout}ms)")

                        # Usar timeout nativo de Playwright (no asyncio.wait_for que no funciona bien con Playwright)
                        await page.goto(result["PlaceUrl"], timeout=card_timeout + 2000)  # +2s extra para navegación

                        # Verificar stop inmediatamente después de goto
                        if should_stop_immediately():
                            logging.info(f"[Celda] Stop detectado después de goto para {city_name}")
                            was_interrupted = True
                            break

                        await page.wait_for_selector('a[data-item-id="authority"]', timeout=card_timeout)

                        # Verificar stop inmediatamente después de wait_for_selector
                        if should_stop_immediately():
                            logging.info(f"[Celda] Stop detectado después de wait_for_selector para {city_name}")
                            was_interrupted = True
                            break

                        logging.debug(f"[Celda] Intentando evaluar website para {result['Name']}")
                        # Extraer website de la ficha
                        website = await page.evaluate('''
                            () => {
                                const websiteButton = document.querySelector('a[data-item-id="authority"]');
                                return websiteButton ? websiteButton.href : "";
                            }
                        ''')
                        logging.debug(f"[Celda] Resultado de evaluate para {result['Name']}: {website}")
                        if website:
                            result["Website"] = website
                            logging.info(f"[Celda] Sitio web encontrado en ficha para {result['Name']}: {website}")
                        else:
                            logging.info(f"[Celda] No se encontró sitio web en ficha para {result['Name']}")

                        # Éxito - considerar bajar el timeout si llevamos varios éxitos
                        consecutive_successes += 1
                        consecutive_timeouts = 0
                        if consecutive_successes >= 5 and current_timeout_idx > 0:
                            current_timeout_idx -= 1
                            consecutive_successes = 0
                            logging.debug(f"[Celda] Bajando timeout a {CARD_TIMEOUTS[current_timeout_idx]}ms tras 5 éxitos")

                    except Exception as e:
                        # Verificar stop también en excepciones
                        if should_stop_immediately():
                            logging.info(f"[Celda] Stop detectado durante excepción en ficha para {city_name}")
                            was_interrupted = True
                            break

                        error_str = str(e).lower()
                        if 'timeout' in error_str:
                            consecutive_timeouts += 1
                            consecutive_successes = 0

                            # Subir timeout si hay fallos consecutivos
                            if consecutive_timeouts >= 2 and current_timeout_idx < len(CARD_TIMEOUTS) - 1:
                                current_timeout_idx += 1
                                consecutive_timeouts = 0
                                logging.debug(f"[Celda] Subiendo timeout a {CARD_TIMEOUTS[current_timeout_idx]}ms tras 2 timeouts")
                        else:
                            consecutive_successes = 0
                        logging.debug(f"[Celda] Error al procesar ficha para {result['Name']}: {e}")

                    # Actualizar contador de fichas abiertas
                    cards_opened_count += 1
                    update_search_progress(cards_opened=cards_opened_count)

                # Si fue interrumpido durante la apertura de ficha, salir del bucle
                if was_interrupted:
                    break

                # Eliminar PlaceUrl antes de añadir al resultado final
                if "PlaceUrl" in result:
                    del result["PlaceUrl"]
                final_results.append(result)

            await context.close()
            return (final_results, was_interrupted)
        except Exception as e:
            logging.error(f"[Celda] Error en scrape_google_maps_cell: {e}")
            return ([], False)

# -----------------------------------------------------
# 5. Scraping de una ciudad
# -----------------------------------------------------

async def scrape_city(city_data: dict, query: str, max_results: int, browser, semaphore, country_code: str, strategy: str = "simple",
                      checkpoint_file: str = None, config: dict = None, processed_cities: set = None,
                      final_csv: str = None, no_emails_csv: str = None, stats_thread=None,
                      control_thread=None, processed_businesses: set = None, temp_processed_businesses: set = None) -> tuple:
    city_name = city_data['name']
    population = city_data.get('population', 0) # Use get with default

    # ==========================================================================
    # CHECKPOINT GRANULAR: Verificar si hay estado guardado para esta ciudad
    # Si existe, usar los negocios del checkpoint en lugar de hacer nueva búsqueda
    # ==========================================================================
    resuming_city = False
    saved_final_results = None
    async with _city_state_lock:
        if (_current_city_state.get('city_name') == city_name and
            _current_city_state.get('raw_businesses') and
            len(_current_city_state.get('processed_indices', set())) > 0):
            # Tenemos estado guardado con progreso - usar negocios del checkpoint
            saved_final_results = _current_city_state['raw_businesses']
            processed_count = len(_current_city_state['processed_indices'])
            total_count = len(saved_final_results)
            logging.info(f"Resumiendo ciudad: {city_name} ({processed_count}/{total_count} negocios ya procesados)")
            resuming_city = True

    # Si estamos resumiendo, usar los negocios guardados; si no, buscar nuevos
    if resuming_city:
        final_results = saved_final_results
    else:
        logging.info(f"Procesando: {city_name} ({population:,} habitantes)")
        # Pass city_data to potentially use its coordinates
        loc = await get_city_location(city_name, country_code, city_data)
        if not loc:
            logging.warning(f"No se pudo obtener la ubicación de {city_name}")
            return ([], False, False)  # (results, interrupted=False, retry=False)

        # Conjunto para mantener registro de fichas visitadas
        visited_places = set()

        # Configuración según estrategia
        if strategy == "simple":
            # Estrategia actual: una celda con tamaño variable
            if population > 500000:
                grid_size = 1
                cell_size_m = 2500   # 2.5 km por celda
                fixed_zoom = 11
            elif population >= 40000:
                grid_size = 1
                cell_size_m = 2000   # 2 km por celda
                fixed_zoom = 12
            else:
                grid_size = 1
                cell_size_m = 1500   # 1.5 km por celda
                fixed_zoom = 12
        else:
            # Estrategia de cuadrícula del otro archivo
            if population > 500000:
                grid_size = 3
                cell_size_m = 2500   # 2.5 km por celda
                fixed_zoom = 15
            elif population >= 40000:
                grid_size = 2
                cell_size_m = 2000   # 2 km por celda
                fixed_zoom = 15
            else:
                grid_size = 1
                cell_size_m = 1500   # 1.5 km por celda
                fixed_zoom = 15

        grid_cells = generate_grid_cells(
            {"center_lat": loc["center_lat"], "center_lon": loc["center_lon"]},
            grid_size,
            cell_size_m,
            strategy
        )

        search_query = f"{query} {city_name}"

        # === VERIFICAR STOP ANTES DE EMPEZAR CELDAS ===
        if control_thread and control_thread.stop_flag:
            logging.info(f"{city_name}: Stop solicitado antes de buscar en Maps")
            return ([], True, False)  # (results, interrupted=True, retry=False)

        cell_tasks = [
            scrape_google_maps_cell(
                search_query, cell, fixed_zoom, max_results, browser, semaphore, visited_places,
                control_thread=control_thread, city_name=city_name, stats_thread=stats_thread
            )
            for cell in grid_cells
        ]
        cell_results_list = await asyncio.gather(*cell_tasks, return_exceptions=True)

        # Verificar si alguna celda fue interrumpida
        any_cell_interrupted = False
        city_results = []
        for cell_result in cell_results_list:
            if isinstance(cell_result, tuple) and len(cell_result) == 2:
                results, was_interrupted = cell_result
                if was_interrupted:
                    any_cell_interrupted = True
                city_results.extend(results)
            elif isinstance(cell_result, list):
                # Compatibilidad con resultados legacy
                city_results.extend(cell_result)

        # Si alguna celda fue interrumpida, retornar inmediatamente
        if any_cell_interrupted:
            logging.info(f"{city_name}: Búsqueda interrumpida durante scraping de celdas")
            return ([], True, False)  # (results, interrupted, retry_requested)

        # Verificar si se solicitó retry de esta ciudad durante la búsqueda
        if control_thread:
            retry_city = control_thread.consume_retry_city()
            if retry_city == city_name:
                logging.info(f"{city_name}: Retry solicitado, reiniciando búsqueda...")
                return ([], False, True)  # (results, interrupted, retry_requested)

        # Primer deduplicado basado en el nombre y dirección antes de procesar sitios web
        unique_results = {}
        for result in city_results:
            if result.get("Name"):
                key = (
                    result.get("Name", "").strip(),
                    result.get("Address", "").strip(),
                    result.get("Phone", "").strip()
                )
                if key not in unique_results:
                    unique_results[key] = result
                elif not unique_results[key].get("Website") and result.get("Website"):
                    # Si ya existe pero no tiene sitio web y el nuevo sí, actualizamos
                    unique_results[key] = result

        final_results = list(unique_results.values())

        # ==========================================================================
        # CHECKPOINT GRANULAR: Guardar estado de ciudad para nueva búsqueda
        # ==========================================================================
        async with _city_state_lock:
            _current_city_state['city_name'] = city_name
            _current_city_state['raw_businesses'] = final_results
            _current_city_state['processed_indices'] = set()
            _current_city_state['enriched_results'] = []

        # IMPORTANTE: Guardar checkpoint INMEDIATAMENTE después de encontrar negocios
        # Esto asegura que si se para antes de completar el primer batch, no se pierden los negocios encontrados
        if checkpoint_file and config and processed_cities is not None:
            logging.debug(f"{city_name}: Guardando checkpoint con {len(final_results)} negocios encontrados")
            await save_city_checkpoint(
                checkpoint_file=checkpoint_file,
                config=config,
                processed_cities=processed_cities,
                final_csv=final_csv,
                no_emails_csv=no_emails_csv,
                stats_thread=stats_thread
            )

    # Crear semáforo para extracciones de email
    extraction_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)

    # Obtener índices ya procesados (para resume)
    already_processed = _current_city_state['processed_indices'].copy()
    enriched_results = _current_city_state['enriched_results'].copy()

    # Filtrar solo los que faltan por procesar
    pending_indices = [i for i in range(len(final_results)) if i not in already_processed]

    if pending_indices:
        logging.info(f"{city_name}: {len(pending_indices)} negocios pendientes de {len(final_results)} total")

        # Actualizar stats con el total de negocios de esta ciudad
        if stats_thread:
            # Actualizar para múltiples workers - cambiar estado a "extracting"
            stats_thread.update_city_progress(
                city_name=city_name,
                businesses_total=len(final_results),
                businesses_processed=len(already_processed),
                status='extracting'
            )
            # Mantener campos legacy para compatibilidad
            stats_thread.update_stats(
                current_city_businesses_total=len(final_results),
                current_city_businesses_processed=len(already_processed)
            )

        # Procesar en batches para poder guardar checkpoint periódicamente
        BATCH_SIZE = 10  # Guardar checkpoint cada 10 negocios

        for batch_start in range(0, len(pending_indices), BATCH_SIZE):
            # === VERIFICAR STOP ANTES DE CADA BATCH ===
            if control_thread and control_thread.stop_flag:
                logging.info(f"{city_name}: Stop solicitado antes de batch, guardando progreso...")
                return (enriched_results, True, False)  # (results, interrupted=True, retry=False)

            batch_indices = pending_indices[batch_start:batch_start + BATCH_SIZE]

            # Crear tareas para este batch
            batch_tasks = []
            for idx in batch_indices:
                result = final_results[idx]
                task = extract_contact_info(
                    result.get("Website", ""),
                    business_name=result.get("Name", ""),
                    extraction_semaphore=extraction_semaphore,
                    current_index=idx + 1,
                    total_urls=len(final_results),
                    browser=browser
                )
                batch_tasks.append((idx, result, task))

            # Ejecutar batch en paralelo con timeout para poder cancelar
            tasks_only = [t[2] for t in batch_tasks]
            try:
                # Timeout de 60 segundos por batch para evitar bloqueos indefinidos
                contact_infos = await asyncio.wait_for(
                    asyncio.gather(*tasks_only, return_exceptions=True),
                    timeout=60.0
                )
            except asyncio.TimeoutError:
                logging.warning(f"{city_name}: Timeout en batch, verificando stop...")
                # Si hay stop, salir; si no, continuar con resultados parciales
                if control_thread and control_thread.stop_flag:
                    return (enriched_results, True, False)
                # Marcar todas como error por timeout
                contact_infos = [{"Email_Raw": "", "Email_Filtered": "", "Email_Search_Status": "Timeout"} for _ in tasks_only]

            # Procesar resultados del batch
            batch_enriched = []  # Resultados enriquecidos de este batch
            for (idx, result, _), info in zip(batch_tasks, contact_infos):
                result["Localidad"] = city_name
                result["Region"] = city_data.get("adminName1", "")
                if isinstance(info, dict):
                    enriched = {**result, **info}
                else:
                    enriched = result.copy()
                    enriched["Email_Raw"] = ""
                    enriched["Email_Filtered"] = ""
                    enriched["Email_Search_Status"] = "Error en enriquecimiento"

                enriched_results.append(enriched)
                batch_enriched.append(enriched)

                # Actualizar estado granular
                async with _city_state_lock:
                    _current_city_state['processed_indices'].add(idx)
                    _current_city_state['enriched_results'].append(enriched)

            # Log de progreso del batch
            processed_count = len(_current_city_state['processed_indices'])
            logging.debug(f"{city_name}: Batch completado - {processed_count}/{len(final_results)} procesados")

            # === GUARDAR RESULTADOS DEL BATCH AL CSV INMEDIATAMENTE ===
            # Esto evita perder datos si se interrumpe el proceso
            if batch_enriched and final_csv and no_emails_csv:
                # Filtrar duplicados usando temp_processed_businesses
                filtered_batch = []
                if temp_processed_businesses is not None:
                    async with _cache_lock:
                        for result in batch_enriched:
                            business_id = get_business_id(result)
                            if business_id and business_id not in temp_processed_businesses:
                                filtered_batch.append(result)
                                temp_processed_businesses.add(business_id)
                else:
                    filtered_batch = batch_enriched

                if not filtered_batch:
                    logging.debug(f"{city_name}: Batch sin resultados nuevos tras filtrar duplicados")
                    continue  # Siguiente batch

                # Crear DataFrame del batch filtrado
                df_batch = pd.DataFrame(filtered_batch)

                # Asegurar columnas necesarias
                required_columns = ['Name', 'Localidad', 'Region', 'Address', 'Phone', 'Rating', 'Website', 'Email_Raw', 'Email_Filtered', 'Email_Search_Status', 'ID']
                for col in required_columns:
                    if col not in df_batch.columns:
                        df_batch[col] = ""
                df_batch = df_batch[required_columns]

                # Separar con y sin emails
                df_with_emails = df_batch[(df_batch["Email_Filtered"].astype(str).str.strip() != "") | (df_batch["Email_Raw"].astype(str).str.strip() != "")].copy()
                df_no_emails = df_batch[
                    (df_batch["Email_Filtered"].astype(str).str.strip() == "") &
                    (df_batch["Email_Raw"].astype(str).str.strip() == "") &
                    (df_batch["Website"].astype(str).str.strip() != "")
                ].copy()

                saved_with_emails = 0
                saved_with_corporate = 0
                saved_with_web = 0

                # Guardar registros con emails
                if not df_with_emails.empty:
                    async with _csv_write_lock:
                        try:
                            start_id = _csv_id_counters['with_emails'] + 1
                            df_with_emails['ID'] = range(start_id, start_id + len(df_with_emails))

                            text_columns = df_with_emails.select_dtypes(include=['object']).columns
                            for col in text_columns:
                                df_with_emails[col] = df_with_emails[col].astype(str).str.replace('nan', '', regex=False).str.replace('None', '', regex=False).str.replace('"', '""')

                            df_with_emails.to_csv(
                                final_csv,
                                mode='a',
                                index=False,
                                header=not os.path.exists(final_csv),
                                encoding='utf-8',
                                quoting=csv.QUOTE_ALL,
                                doublequote=True
                            )
                            _csv_id_counters['with_emails'] = start_id + len(df_with_emails) - 1
                            saved_with_emails = len(df_with_emails)
                            saved_with_corporate = len(df_with_emails[df_with_emails["Email_Filtered"].astype(str).str.strip() != ""])
                        except Exception as e:
                            logging.error(f"Error guardando batch con emails: {e}")

                # Guardar registros sin emails pero con web
                if not df_no_emails.empty:
                    async with _csv_write_lock:
                        try:
                            start_id = _csv_id_counters['no_emails'] + 1
                            df_no_emails['ID'] = range(start_id, start_id + len(df_no_emails))

                            text_columns = df_no_emails.select_dtypes(include=['object']).columns
                            for col in text_columns:
                                df_no_emails[col] = df_no_emails[col].astype(str).str.replace('nan', '', regex=False).str.replace('None', '', regex=False).str.replace('"', '""')

                            df_no_emails.to_csv(
                                no_emails_csv,
                                mode='a',
                                index=False,
                                header=not os.path.exists(no_emails_csv),
                                encoding='utf-8',
                                quoting=csv.QUOTE_ALL,
                                doublequote=True
                            )
                            _csv_id_counters['no_emails'] = start_id + len(df_no_emails) - 1
                            saved_with_web = len(df_no_emails)
                        except Exception as e:
                            logging.error(f"Error guardando batch sin emails: {e}")

                # Actualizar stats con los registros guardados en este batch
                if stats_thread and (saved_with_emails > 0 or saved_with_web > 0):
                    current_stats = stats_thread.get_stats()
                    stats_thread.update_stats(
                        results_total=current_stats.results_total + saved_with_emails + saved_with_web,
                        results_with_email=current_stats.results_with_email + saved_with_emails,
                        results_with_web=current_stats.results_with_web + saved_with_web,
                        results_with_corporate_email=current_stats.results_with_corporate_email + saved_with_corporate
                    )
                    logging.debug(f"{city_name}: Batch guardado - {saved_with_emails} con email, {saved_with_web} con web")

            # Actualizar stats con progreso granular después de cada batch
            if stats_thread:
                # Actualizar para múltiples workers
                stats_thread.update_city_progress(
                    city_name=city_name,
                    businesses_processed=processed_count
                )
                # Mantener campo legacy
                stats_thread.update_stats(
                    current_city_businesses_processed=processed_count
                )

            # CHECKPOINT GRANULAR: Guardar estado después de cada batch
            if checkpoint_file and config and processed_cities is not None:
                await save_city_checkpoint(
                    checkpoint_file=checkpoint_file,
                    config=config,
                    processed_cities=processed_cities,
                    final_csv=final_csv,
                    no_emails_csv=no_emails_csv,
                    stats_thread=stats_thread
                )

            # PARADA GRANULAR: Verificar flags de control después de cada batch
            if control_thread and (control_thread.stop_flag or control_thread.pause_flag):
                logging.info(f"{city_name}: Parada/pausa solicitada después de batch ({processed_count}/{len(final_results)} procesados)")
                # Retornar resultados parciales con flag de interrupción
                return (enriched_results, True, False)  # (results, interrupted=True, retry=False)

    else:
        logging.info(f"{city_name}: Todos los negocios ya procesados (resume)")

    # Deduplicar final basándose en Website y Phone
    seen = set()
    deduped_results = []
    for r in enriched_results:
        key = (r.get("Website", "").strip(), r.get("Phone", "").strip())
        if key not in seen:
            seen.add(key)
            deduped_results.append(r)

    # Limpiar estado de ciudad (ya completada)
    clear_city_state()

    # Mostrar resumen de resultados
    total = len(deduped_results)
    with_emails = len([r for r in deduped_results if r.get("Email_Filtered")])
    with_web = len([r for r in deduped_results if r.get("Website") and not r.get("Email_Filtered")])
    logging.info(f"{city_name}: {total} resultados ({with_emails} con email, {with_web} con web)")

    return (deduped_results, False, False)  # (results, interrupted=False, retry=False)

# -----------------------------------------------------
# 6. Función principal (main)
# -----------------------------------------------------

async def process_city(city_data: dict, config: dict, browser, semaphore, processed_businesses: set, temp_processed_businesses: set, cache_file: str,
                      final_csv: str, no_emails_csv: str, processed_cities: set, checkpoint_file: str, stats_thread=None,
                      control_thread=None) -> tuple:
    """Procesa una ciudad. Retorna (interrupted, retry_requested)."""
    city_name = city_data['name']
    country_code = config['country_code']
    strategy = config['strategy']
    query = config['query']
    max_results = config['max_results']

    try:
        logging.info(f"\nProcesando ciudad '{city_name}'. Población: {city_data.get('population', 'N/A'):,}")

        results, interrupted, retry_requested = await scrape_city(
            city_data=city_data,
            query=query,
            max_results=max_results,
            browser=browser,
            semaphore=semaphore,
            country_code=country_code,
            strategy=strategy,
            checkpoint_file=checkpoint_file,
            config=config,
            processed_cities=processed_cities,
            final_csv=final_csv,
            no_emails_csv=no_emails_csv,
            stats_thread=stats_thread,
            control_thread=control_thread,
            processed_businesses=processed_businesses,
            temp_processed_businesses=temp_processed_businesses
        )

        # Si se solicitó retry, limpiar estado y señalar para reencolar
        if retry_requested:
            logging.info(f"{city_name}: Retry solicitado, limpiando estado...")
            clear_city_state()  # Limpiar estado de la ciudad
            if stats_thread:
                stats_thread.remove_active_city(city_name)
            return (False, True)  # (interrupted=False, retry=True)

        # Si fue interrumpida, no procesar resultados ni marcar como completada
        if interrupted:
            logging.info(f"{city_name}: Ciudad interrumpida, progreso guardado en checkpoint")
            return (True, False)  # (interrupted=True, retry=False)

        if results:
            # Filtrar negocios usando el registro temporal para evitar duplicados entre workers
            # OPTIMIZACIÓN v2: Usar lock compartido global
            async with _cache_lock:
                for result in results:
                    business_id = get_business_id(result)
                    if business_id and business_id not in temp_processed_businesses:
                        temp_processed_businesses.add(business_id)

            # Persist the updated full set of processed businesses
            async with _cache_lock:
                processed_businesses.update(temp_processed_businesses)
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(list(processed_businesses), f)
                except Exception as e:
                    logging.error(f"Error guardando caché: {e}")

            # Mostrar resumen de la ciudad completada
            # NOTA: Los resultados ya se guardaron al CSV en cada batch dentro de scrape_city
            total = len(results)
            with_emails = sum(1 for r in results if r.get("Email_Raw") or r.get("Email_Filtered"))
            with_corporate = sum(1 for r in results if r.get("Email_Filtered"))
            with_web = sum(1 for r in results if r.get("Website") and not r.get("Email_Raw") and not r.get("Email_Filtered"))

            log_section(f"Ciudad completada: {city_name}")
            log_stats("Total procesados:", total)
            log_stats("Con email (guardados):", with_emails)
            log_stats("  - Corporativos:", with_corporate)
            log_stats("Con web sin email:", with_web)

        # Actualizar checkpoint (ciudad completada)
        # Usa save_city_checkpoint que incluye estado granular
        processed_cities.add(city_name)
        await save_city_checkpoint(
            checkpoint_file=checkpoint_file,
            config=config,
            processed_cities=processed_cities,
            final_csv=final_csv,
            no_emails_csv=no_emails_csv,
            stats_thread=stats_thread
        )

        return (False, False)  # (interrupted=False, retry=False) - Ciudad completada

    except Exception as e:
        logging.error(f"Error procesando ciudad {city_name}: {e}", exc_info=True)
        return (False, False)  # (interrupted=False, retry=False) - Error pero continuar

async def main(stdscr): # Keep stdscr for potential dashboard use
    # Initialize config dictionary
    config = {}
    args = None # Initialize args
    worker_tasks = [] # Define worker_tasks list early
    browser = None # Define browser early
    shutdown_event = asyncio.Event() # Event to signal shutdown
    granular_interruption = False  # Flag para indicar parada a mitad de ciudad
    dashboard_thread = None # Initialize thread var

    # Job Manager variables
    job_manager = None
    current_job = None
    control_thread = None
    stats_thread = None

    # --- Signal Handler --- #
    # This allows asyncio to properly handle SIGINT/SIGTERM
    loop = asyncio.get_running_loop()

    def ask_exit(signame):
        logging.info(f"\nSeñal {signame} recibida, iniciando parada elegante...")
        # Check if shutdown already requested to avoid double execution
        if not shutdown_event.is_set():
            shutdown_event.set() # Signal workers and main loop

    try:
        # Add signal handlers for graceful shutdown
        for signame in ('SIGINT', 'SIGTERM'):
            # Use add_signal_handler for async compatibility
            loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: ask_exit(signame)
            )
    except NotImplementedError:
        # add_signal_handler is not implemented on Windows for SIGTERM
        # SIGINT (Ctrl+C) should still work via default asyncio handling
        logging.debug("add_signal_handler para SIGTERM no soportado en este sistema (Windows?). SIGINT (Ctrl+C) debería funcionar.")
        # We still rely on the try/except KeyboardInterrupt later for Ctrl+C on Windows

    try:
        # Basic argparse for non-interactive parameters or overrides
        parser = argparse.ArgumentParser(
            description='Scraper Interactivo de Google Maps usando GeoNames API.'
        )
        # Update default value logic for geonames_username
        parser.add_argument('--geonames_username', type=str,
                            default=os.environ.get('GEONAMES_USERNAME', 'gorkota'), # Prioritize ENV, then 'gorkota'
                            help='Nombre de usuario de GeoNames (o variable de entorno GEONAMES_USERNAME, por defecto: gorkota)')
        parser.add_argument('--resume', action='store_true',
                            help='Retomar la ejecución desde el último checkpoint (en primer plano)')
        parser.add_argument('--resume-background', action='store_true', dest='resume_background',
                            help='Retomar la ejecución desde el último checkpoint (en segundo plano)')
        # Add optional args to override interactive prompts if needed
        parser.add_argument('--country_code', type=str, help='(Opcional) Código de país (ej. ES, GB) para saltar pregunta.')
        # --region argument now expects region *code* (adminCode1) if provided via CLI
        parser.add_argument('--region_code', type=str, help='(Opcional) Código de región/provincia (ADM1) para saltar pregunta.')
        parser.add_argument('--adm2_code', type=str, help='(Opcional) Código de subdivisión (ADM2, ej. provincia en ES) para saltar pregunta.') # New ADM2 argument
        parser.add_argument('--query', type=str, help='(Opcional) Término de búsqueda para saltar pregunta.')
        parser.add_argument('--min_population', type=int, help='(Opcional) Población mínima para saltar pregunta.')
        parser.add_argument('--max_population', type=int, help='(Opcional) Población máxima (exclusiva) para saltar pregunta.') # New max_pop argument
        parser.add_argument('--strategy', type=str, choices=['simple', 'grid'], help='(Opcional) Estrategia (simple/grid) para saltar pregunta.')
        parser.add_argument('--max_results', type=int, help='(Opcional) Máx resultados por celda para saltar pregunta.')
        parser.add_argument('--workers', type=int, help='(Opcional) Número de workers en paralelo.')
        parser.add_argument('--batch', action='store_true',
                            help='Modo batch/background: no pide input interactivo. Usa valores por defecto para lo no especificado.')
        parser.add_argument('--setup', action='store_true',
                            help='Configuración interactiva completa + verificación de Google Maps en navegador visible. Guarda todo para --continue.')
        parser.add_argument('--reset-cookies', action='store_true', dest='reset_cookies',
                            help='Elimina las cookies guardadas de Google Maps para reiniciar la sesión.')
        parser.add_argument('--continue-run', action='store_true', dest='continue_run',
                            help='Relanza el scraping en segundo plano usando la configuración guardada por --setup.')
        parser.add_argument('--run-id', type=str, dest='run_id',
                            help='ID de ejecución específico para retomar con --resume. Si no se especifica, retoma el más reciente.')

        args = parser.parse_args()

        # --- Rutas de archivos de configuración persistente ---
        script_dir = os.path.dirname(os.path.abspath(__file__))
        browser_data_dir = os.path.join(script_dir, "browser_data")
        os.makedirs(browser_data_dir, exist_ok=True)
        storage_state_path = os.path.join(browser_data_dir, "google_maps_state.json")
        setup_config_path = os.path.join(browser_data_dir, "setup_config.json")

        # --- Modo --reset-cookies: eliminar cookies guardadas ---
        if args.reset_cookies:
            files_deleted = []
            if os.path.exists(storage_state_path):
                os.remove(storage_state_path)
                files_deleted.append("cookies de Google Maps")

            if files_deleted:
                print(f"\n✓ Sesión reiniciada correctamente.")
                print(f"  Eliminado: {', '.join(files_deleted)}")
                print(f"\nEjecuta --setup para configurar una nueva sesión.")
            else:
                print("\nNo había cookies guardadas para eliminar.")
            return

        # --- Modo --continue: relanzar con config guardada ---
        if args.continue_run:
            if not os.path.exists(setup_config_path):
                print("ERROR: No se encontró configuración guardada. Ejecuta primero con --setup.")
                return
            if not os.path.exists(storage_state_path):
                print("ERROR: No se encontraron cookies de Google Maps. Ejecuta primero con --setup.")
                return

            with open(setup_config_path, 'r', encoding='utf-8') as f:
                saved_config = json.load(f)

            print("\n--- Configuración guardada del setup ---")
            print(f"  Usuario GeoNames:   {saved_config.get('geonames_username')}")
            print(f"  País:               {saved_config.get('country_code')}")
            print(f"  Región:             {saved_config.get('region_name', 'Todas')}")
            print(f"  Búsqueda Maps:      {saved_config.get('query')}")
            print(f"  Población mínima:   {saved_config.get('min_population', 1000):,}")
            max_pop = saved_config.get('max_population')
            print(f"  Población máxima:   {f'{max_pop:,}' if max_pop else 'Sin límite'}")
            print(f"  Estrategia:         {saved_config.get('strategy', 'simple')}")
            print(f"  Máx. resultados:    {saved_config.get('max_results', 50)}")
            print(f"  Workers:            {saved_config.get('workers', 1)}")
            print("-" * 40)

            # Construir comando para lanzar en background
            cmd_parts = [
                sys.executable, os.path.abspath(__file__),
                '--batch',
                '--geonames_username', saved_config['geonames_username'],
                '--country_code', saved_config['country_code'],
                '--query', saved_config['query'],
                '--min_population', str(saved_config.get('min_population', 1000)),
                '--strategy', saved_config.get('strategy', 'simple'),
                '--max_results', str(saved_config.get('max_results', 50)),
                '--workers', str(saved_config.get('workers', 1)),
            ]
            if saved_config.get('region_code'):
                cmd_parts.extend(['--region_code', saved_config['region_code']])
            if saved_config.get('adm2_code'):
                cmd_parts.extend(['--adm2_code', saved_config['adm2_code']])
            if saved_config.get('max_population') is not None:
                cmd_parts.extend(['--max_population', str(saved_config['max_population'])])

            log_file = os.path.join(script_dir, "scrapping_background.log")
            print(f"\nLanzando scraping en segundo plano...")
            print(f"Log: {log_file}")
            print(f"Para ver progreso: tail -f {log_file}")

            with open(log_file, 'w') as lf:
                proc = subprocess.Popen(
                    cmd_parts,
                    stdout=lf, stderr=subprocess.STDOUT,
                    start_new_session=True
                )
            print(f"Proceso lanzado con PID: {proc.pid}")
            print("Puedes cerrar esta terminal. El scraping continuará en segundo plano.")
            return

        # --- Modo --resume-background: retomar en segundo plano ---
        if args.resume_background:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            log_file = os.path.join(script_dir, "scrapping_background.log")

            cmd_parts = [
                sys.executable, os.path.abspath(__file__),
                '--resume',
                '--batch'  # Para evitar cualquier input interactivo
            ]

            print(f"\nRetomando scraping en segundo plano...")
            print(f"Log: {log_file}")
            print(f"Para ver progreso: tail -f {log_file}")

            with open(log_file, 'a') as lf:  # Append para no perder logs anteriores
                lf.write(f"\n{'='*60}\n")
                lf.write(f"Retomando ejecución: {datetime.now().isoformat()}\n")
                lf.write(f"{'='*60}\n")
                proc = subprocess.Popen(
                    cmd_parts,
                    stdout=lf, stderr=subprocess.STDOUT,
                    start_new_session=True
                )
            print(f"Proceso lanzado con PID: {proc.pid}")
            print("Puedes cerrar esta terminal. El scraping continuará en segundo plano.")
            return

        # --- Modo Setup: configuración interactiva + verificación Google Maps ---
        if args.setup:
            print("\n=== SETUP INICIAL ===")
            print("Configurarás los parámetros de búsqueda y luego se abrirá")
            print("Google Maps para que aceptes cookies/verificaciones.\n")

            setup_config = {}
            setup_config['geonames_username'] = os.environ.get('GEONAMES_USERNAME', 'gorkota')

            # 1. GeoNames Username
            username_input = input(f"Usuario de GeoNames (Enter para '{setup_config['geonames_username']}'): ").strip()
            if username_input:
                setup_config['geonames_username'] = username_input

            # 2. Country Code
            while not setup_config.get('country_code'):
                code = input("Código de país (2 letras, ej: ES, GB): ").strip().upper()
                if len(code) == 2 and code.isalpha():
                    setup_config['country_code'] = code
                else:
                    print("Código de país inválido. Deben ser 2 letras.")

            # 3. Region (ADM1) - opcional
            filter_adm1 = input("¿Filtrar por Región/Estado? (s/N): ").strip().lower()
            if filter_adm1 == 's':
                regions = await get_regions_for_country(setup_config['country_code'], setup_config['geonames_username'])
                if regions:
                    print(f"\nRegiones disponibles:")
                    for i, r in enumerate(regions):
                        print(f"  {i+1}. {r['name']} ({r.get('code', 'N/A')})")
                    while True:
                        choice = input("Selecciona número o nombre (Enter para todas): ").strip()
                        if not choice:
                            break
                        items_by_num = {str(i+1): r for i, r in enumerate(regions)}
                        items_by_name = {r['name'].lower(): r for r in regions}
                        chosen = items_by_num.get(choice) or items_by_name.get(choice.lower())
                        if chosen and chosen.get('code'):
                            setup_config['region_code'] = chosen['code']
                            setup_config['region_name'] = chosen['name']
                            print(f"Seleccionado: {chosen['name']} ({chosen['code']})")

                            # 3b. ADM2 - opcional
                            filter_adm2 = input(f"¿Filtrar por Provincia dentro de '{chosen['name']}'? (s/N): ").strip().lower()
                            if filter_adm2 == 's':
                                subdivisions = await get_admin_level_2_divisions(setup_config['country_code'], chosen['code'], setup_config['geonames_username'])
                                if subdivisions:
                                    print(f"\nProvincias disponibles:")
                                    for i, s in enumerate(subdivisions):
                                        print(f"  {i+1}. {s['name']} ({s.get('code', 'N/A')})")
                                    while True:
                                        choice2 = input("Selecciona número o nombre (Enter para todas): ").strip()
                                        if not choice2:
                                            break
                                        items2_by_num = {str(i+1): s for i, s in enumerate(subdivisions)}
                                        items2_by_name = {s['name'].lower(): s for s in subdivisions}
                                        chosen2 = items2_by_num.get(choice2) or items2_by_name.get(choice2.lower())
                                        if chosen2 and chosen2.get('code'):
                                            setup_config['adm2_code'] = chosen2['code']
                                            setup_config['adm2_name'] = chosen2['name']
                                            print(f"Seleccionado: {chosen2['name']} ({chosen2['code']})")
                                            break
                                        print("Selección inválida.")
                            break
                        print("Selección inválida.")

            # 4. Query
            while not setup_config.get('query'):
                query_term = input("¿Qué buscar en Google Maps? (ej: restaurantes, hoteles): ").strip()
                if query_term:
                    setup_config['query'] = query_term
                else:
                    print("El término de búsqueda es obligatorio.")

            # 5. Población mínima
            default_min = 1000
            pop_input = input(f"Población mínima (Enter para {default_min:,}): ").strip()
            setup_config['min_population'] = int(pop_input) if pop_input.isdigit() else default_min

            # 6. Población máxima
            pop_max_input = input("Población máxima (Enter para sin límite): ").strip()
            setup_config['max_population'] = int(pop_max_input) if pop_max_input.isdigit() else None

            # 7. Estrategia
            strat_input = input("Estrategia [simple/grid] (Enter para 'simple'): ").strip().lower()
            setup_config['strategy'] = strat_input if strat_input in ['simple', 'grid'] else 'simple'

            # 8. Max resultados
            max_res_input = input("Máx. resultados por celda (Enter para 50): ").strip()
            setup_config['max_results'] = int(max_res_input) if max_res_input.isdigit() and int(max_res_input) > 0 else 50

            # 9. Workers
            workers_input = input("Workers concurrentes (Enter para 1): ").strip()
            setup_config['workers'] = int(workers_input) if workers_input.isdigit() and int(workers_input) > 0 else 1

            # --- Resumen ---
            print("\n--- Resumen de Configuración ---")
            print(f"  Usuario GeoNames:   {setup_config['geonames_username']}")
            print(f"  País:               {setup_config['country_code']}")
            print(f"  Región:             {setup_config.get('region_name', 'Todas')}")
            if setup_config.get('adm2_name'):
                print(f"  Provincia:          {setup_config['adm2_name']}")
            print(f"  Búsqueda Maps:      {setup_config['query']}")
            print(f"  Población mínima:   {setup_config['min_population']:,}")
            max_pop = setup_config.get('max_population')
            print(f"  Población máxima:   {f'{max_pop:,}' if max_pop else 'Sin límite'}")
            print(f"  Estrategia:         {setup_config['strategy']}")
            print(f"  Máx. resultados:    {setup_config['max_results']}")
            print(f"  Workers:            {setup_config['workers']}")
            print("-" * 40)

            confirm = input("\n¿Guardar esta configuración y abrir Google Maps? (S/n): ").strip().lower()
            if confirm not in ('s', ''):
                print("Setup cancelado.")
                return

            # Guardar configuración
            with open(setup_config_path, 'w', encoding='utf-8') as f:
                json.dump(setup_config, f, ensure_ascii=False, indent=2)
            print(f"\nConfiguración guardada en: {setup_config_path}")

            # Abrir navegador visible para verificación
            print("\nAbriendo Google Maps en navegador visible...")
            print("  1. Acepta las cookies/consentimiento de Google")
            print("  2. Verifica que ves resultados en el mapa")
            print("  3. Cierra el navegador cuando termines\n")

            async with async_playwright() as p_setup:
                # Usar viewport más pequeño para compatibilidad con laptops
                # 1280x720 es un tamaño seguro que cabe en la mayoría de pantallas
                setup_browser = await p_setup.chromium.launch(
                    headless=False,
                    args=['--lang=es-ES', '--window-size=1300,750']
                )
                setup_context = await setup_browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    viewport={'width': 1280, 'height': 720}
                )
                setup_page = await setup_context.new_page()
                # Navegar con la query real del usuario
                test_query = setup_config['query'].replace(' ', '+')
                await setup_page.goto(f"https://www.google.com/maps/search/{test_query}/@40.4165,-3.70256,12z")

                try:
                    await setup_page.wait_for_event("close", timeout=300000)
                except Exception:
                    pass

                try:
                    await setup_context.storage_state(path=storage_state_path)
                    print(f"\nCookies guardadas correctamente.")
                except Exception:
                    print("\nAVISO: No se pudieron guardar las cookies.")

                await setup_context.close()
                await setup_browser.close()

            print("\n" + "=" * 50)
            print("SETUP COMPLETADO")
            print("=" * 50)
            print(f"\nPara lanzar el scraping en segundo plano, ejecuta:")
            print(f"  python {os.path.basename(__file__)} --continue-run")
            print(f"\nEl proceso correrá sin necesitar terminal ni intervención.")
            return

        # ---- Resume Logic ----
        processed_cities = set()
        processed_businesses = set()
        temp_processed_businesses = set()
        final_csv = ""
        no_emails_csv = ""
        checkpoint_file = None # Initialize
        cache_file = None # Initialize
        resuming = False # Flag to track if resume was successful
        current_run_id = None  # Track the run_id for this execution
        base_stats = None  # Stats acumuladas de ejecuciones anteriores (para resume)

        if args.resume:
            cache_dir = "cache"
            os.makedirs(cache_dir, exist_ok=True)

            # If a specific run_id is provided, look for that checkpoint
            if args.run_id:
                print(f"Buscando checkpoint para run_id: {args.run_id}...")
                checkpoint_file = os.path.join(cache_dir, f"checkpoint_{args.run_id}.json")
                if not os.path.exists(checkpoint_file):
                    print(f"No se encontró checkpoint para run_id '{args.run_id}'.")
                    # List available checkpoints
                    available = [f for f in os.listdir(cache_dir) if f.startswith('checkpoint_') and f.endswith('.json')]
                    if available:
                        print("\nCheckpoints disponibles:")
                        for cp in sorted(available, key=lambda x: os.path.getmtime(os.path.join(cache_dir, x)), reverse=True)[:10]:
                            run_id_match = re.match(r"checkpoint_(.*)\.json", cp)
                            if run_id_match:
                                cp_path = os.path.join(cache_dir, cp)
                                try:
                                    with open(cp_path, 'r') as f:
                                        cp_data = json.load(f)
                                    cp_config = cp_data.get('config', {})
                                    print(f"  - {run_id_match.group(1)}: {cp_config.get('query', '?')} en {cp_config.get('country_code', '?')}")
                                except:
                                    print(f"  - {run_id_match.group(1)}: (error leyendo)")
                    args.resume = False
                    checkpoint_file = None
            else:
                print("Buscando ejecuciones disponibles para retomar...")

                # Usar JobManager para obtener lista de jobs (consistente con scrape_manager)
                temp_job_manager = JobManager()
                all_jobs = temp_job_manager.list_jobs()

                # Filtrar jobs no completados (con o sin checkpoint)
                resumable_jobs = []
                for job in all_jobs:
                    if job.status not in [JobStatus.COMPLETED]:
                        # Marcar si tiene checkpoint existente
                        job._has_checkpoint = job.checkpoint_file and os.path.exists(job.checkpoint_file)
                        resumable_jobs.append(job)

                if not resumable_jobs:
                    print("No se encontraron ejecuciones para retomar.")
                    print("Tip: Usa 'python scrape_manager.py' para ver el historial completo.")
                    args.resume = False
                elif len(resumable_jobs) == 1:
                    # Solo hay uno, usarlo directamente
                    job = resumable_jobs[0]
                    if job._has_checkpoint:
                        checkpoint_file = job.checkpoint_file
                        print(f"Única ejecución encontrada: {job.job_id}")
                        print(f"  Query: {job.config.get('query', '?')} en {job.config.get('country_code', '?')}")
                    else:
                        # Sin checkpoint - usar config para empezar de cero
                        checkpoint_file = None
                        config = job.config
                        print(f"Única ejecución encontrada (SIN CHECKPOINT - comenzará de cero): {job.job_id}")
                        print(f"  Query: {config.get('query', '?')} en {config.get('country_code', '?')}")
                        # Configurar args desde la config del job
                        args.resume = False  # No es resume real, es relanzar
                        args.country_code = config.get('country_code')
                        args.query = config.get('query')
                        args.min_population = config.get('min_population', 50000)
                        args.max_population = config.get('max_population')
                        args.strategy = config.get('strategy', 'simple')
                        args.max_results = config.get('max_results', 300)
                        args.workers = config.get('workers', 1)
                        args.region_code = config.get('region_code')
                        args.adm2_code = config.get('adm2_code')
                else:
                    # Múltiples jobs - mostrar selector interactivo
                    print(f"\n{'='*70}")
                    print("EJECUCIONES DISPONIBLES PARA RETOMAR")
                    print(f"{'='*70}")

                    for i, job in enumerate(resumable_jobs[:10]):  # Máximo 10
                        query = job.config.get('query', '?')
                        country = job.config.get('country_code', '?')
                        cities_done = job.stats.cities_processed
                        cities_total = job.stats.cities_total
                        status = job.status
                        runtime = format_duration(job.stats.runtime_seconds)

                        # Indicar si tiene checkpoint o no
                        if job._has_checkpoint:
                            checkpoint_indicator = ""
                            # Verificar si hay ciudad en progreso en el checkpoint
                            city_prog = ""
                            try:
                                with open(job.checkpoint_file, 'r', encoding='utf-8') as f:
                                    cp_data = json.load(f)
                                city_in_prog = cp_data.get('city_in_progress')
                                if city_in_prog and city_in_prog.get('city_name'):
                                    processed = len(city_in_prog.get('processed_indices', []))
                                    total = len(city_in_prog.get('raw_businesses', []))
                                    city_prog = f" -> {city_in_prog['city_name']} ({processed}/{total} negocios)"
                            except Exception:
                                pass
                        else:
                            checkpoint_indicator = " [SIN CHECKPOINT]"
                            city_prog = ""

                        status_str = f"[{status.upper()}]" if status != JobStatus.INTERRUPTED else "[INTERRUMPIDO]"
                        print(f"  [{i+1}] {job.job_id}: {query} en {country}{checkpoint_indicator}")
                        print(f"      {status_str} {cities_done}/{cities_total} ciudades | {runtime}{city_prog}")

                    print(f"\n  [0] Cancelar")
                    print(f"{'='*70}")

                    # Pedir selección (solo en modo interactivo)
                    if not args.batch:
                        try:
                            selection = input("\nSelecciona ejecución a retomar [1]: ").strip()
                            if selection == "" or selection == "1":
                                idx = 0
                            elif selection == "0":
                                print("Operación cancelada.")
                                return
                            else:
                                idx = int(selection) - 1
                                if idx < 0 or idx >= len(resumable_jobs):
                                    print("Selección inválida.")
                                    return

                            selected_job = resumable_jobs[idx]
                            if selected_job._has_checkpoint:
                                checkpoint_file = selected_job.checkpoint_file
                                print(f"\nRetomando: {selected_job.job_id} - {selected_job.config.get('query')} en {selected_job.config.get('country_code')}")
                            else:
                                # Sin checkpoint - usar config para empezar de cero
                                checkpoint_file = None
                                config = selected_job.config
                                print(f"\nRelanzando desde cero (sin checkpoint): {selected_job.job_id}")
                                print(f"  Query: {config.get('query')} en {config.get('country_code')}")
                                args.resume = False
                                args.country_code = config.get('country_code')
                                args.query = config.get('query')
                                args.min_population = config.get('min_population', 50000)
                                args.max_population = config.get('max_population')
                                args.strategy = config.get('strategy', 'simple')
                                args.max_results = config.get('max_results', 300)
                                args.workers = config.get('workers', 1)
                                args.region_code = config.get('region_code')
                                args.adm2_code = config.get('adm2_code')
                                # Eliminar el job viejo para evitar duplicados
                                temp_job_manager.delete_job(selected_job.job_id)
                        except ValueError:
                            print("Entrada inválida.")
                            return
                        except KeyboardInterrupt:
                            print("\nCancelado.")
                            return
                    else:
                        # En modo batch, usar el más reciente con checkpoint
                        jobs_with_checkpoint = [j for j in resumable_jobs if j._has_checkpoint]
                        if jobs_with_checkpoint:
                            checkpoint_file = jobs_with_checkpoint[0].checkpoint_file
                            print(f"\nModo batch: usando ejecución más reciente con checkpoint ({jobs_with_checkpoint[0].job_id})")
                        else:
                            # No hay ninguno con checkpoint, usar el más reciente
                            selected_job = resumable_jobs[0]
                            checkpoint_file = None
                            config = selected_job.config
                            print(f"\nModo batch: relanzando desde cero ({selected_job.job_id})")
                            args.resume = False
                            args.country_code = config.get('country_code')
                            args.query = config.get('query')
                            args.min_population = config.get('min_population', 50000)
                            args.max_population = config.get('max_population')
                            args.strategy = config.get('strategy', 'simple')
                            args.max_results = config.get('max_results', 300)
                            args.workers = config.get('workers', 1)
                            args.region_code = config.get('region_code')
                            args.adm2_code = config.get('adm2_code')
                            temp_job_manager.delete_job(selected_job.job_id)

            if checkpoint_file and os.path.exists(checkpoint_file):
                try:
                    logging.info(f"Cargando checkpoint desde: {checkpoint_file}")
                    with open(checkpoint_file, 'r', encoding='utf-8') as f:
                        checkpoint_data = json.load(f)

                    # Extract run_id from checkpoint filename or data
                    run_id_match = re.match(r"checkpoint_(.*)\.json", os.path.basename(checkpoint_file))
                    if run_id_match:
                        current_run_id = run_id_match.group(1)
                    else:
                        current_run_id = checkpoint_data.get('run_id')

                    logging.info(f"Run ID: {current_run_id}")

                    # Load config from checkpoint FIRST
                    if 'config' in checkpoint_data:
                        config = checkpoint_data['config']
                        logging.info("Configuración cargada desde checkpoint.")
                        # Ensure resume flag is set in the loaded config
                        config['resume'] = True
                        # Make sure essential keys exist
                        config.setdefault('workers', DEFAULT_WORKERS)
                    else:
                         raise ValueError("El checkpoint no contiene la configuración necesaria ('config').")

                    processed_cities = set(checkpoint_data.get('processed_cities', []))
                    final_csv = checkpoint_data.get('final_csv')
                    no_emails_csv = checkpoint_data.get('no_emails_csv')

                    # Cargar stats acumuladas del checkpoint (si existen)
                    accumulated_stats_data = checkpoint_data.get('accumulated_stats')
                    if accumulated_stats_data:
                        base_stats = JobStats(
                            runtime_seconds=accumulated_stats_data.get('runtime_seconds', 0),
                            results_total=accumulated_stats_data.get('results_total', 0),
                            results_with_email=accumulated_stats_data.get('results_with_email', 0),
                            results_with_corporate_email=accumulated_stats_data.get('results_with_corporate_email', 0),
                            results_with_web=accumulated_stats_data.get('results_with_web', 0),
                            errors_count=accumulated_stats_data.get('errors_count', 0),
                            avg_city_duration=accumulated_stats_data.get('avg_city_duration', 0),
                            cities_total=0,  # Se actualizará después
                            cities_processed=len(processed_cities),
                        )
                        logging.info(f"  - Stats acumuladas: {format_duration(base_stats.runtime_seconds)} runtime, {base_stats.results_total} resultados")
                    else:
                        base_stats = None
                        logging.info("  - Sin stats acumuladas previas (checkpoint antiguo)")

                    # Ensure file paths are valid
                    if not final_csv or not no_emails_csv:
                        raise ValueError("Nombres de archivo CSV no encontrados en el checkpoint.")

                    # Cache file uses the same run_id
                    cache_file = os.path.join(cache_dir, f"processed_businesses_{current_run_id}.json")

                    logging.info(f"Retomando ejecución con la configuración del checkpoint.")
                    logging.info(f"  - Run ID: {current_run_id}")
                    logging.info(f"  - {len(processed_cities)} ciudades ya procesadas.")
                    logging.info(f"  - Archivo con correos: {final_csv}")
                    logging.info(f"  - Archivo sin correos: {no_emails_csv}")
                    logging.info(f"  - Archivo caché: {cache_file}")

                    # Load processed businesses cache associated with this checkpoint
                    if os.path.exists(cache_file):
                        try:
                            with open(cache_file, 'r', encoding='utf-8') as f:
                                processed_businesses = set(json.load(f))
                            logging.info(f"Cargados {len(processed_businesses)} IDs de negocios desde {cache_file}")
                            temp_processed_businesses = processed_businesses.copy() # Sync temp cache
                        except Exception as e:
                            logging.error(f"Error cargando caché de negocios ({cache_file}): {e}. Se continuará sin caché.")
                            processed_businesses = set() # Reset cache if loading failed
                            temp_processed_businesses = set()
                    else:
                        logging.warning(f"No se encontró archivo de caché {cache_file} asociado al checkpoint.")

                    # CHECKPOINT GRANULAR: Restaurar estado de ciudad en progreso (si existe)
                    city_in_progress_data = checkpoint_data.get('city_in_progress')
                    if city_in_progress_data and city_in_progress_data.get('city_name'):
                        restore_city_state(city_in_progress_data)
                        city_name = city_in_progress_data.get('city_name')
                        processed_count = len(city_in_progress_data.get('processed_indices', []))
                        total_count = len(city_in_progress_data.get('raw_businesses', []))
                        logging.info(f"  - Ciudad en progreso: {city_name} ({processed_count}/{total_count} negocios)")
                    else:
                        clear_city_state()  # Asegurar estado limpio

                    resuming = True # Set resume flag to true

                    # OPTIMIZACIÓN v2: Inicializar contadores de ID desde CSV existentes
                    # para evitar leer el CSV completo en cada escritura
                    # También actualizar las stats con los valores REALES del CSV
                    csv_email_count = 0
                    csv_no_email_count = 0

                    if final_csv and os.path.exists(final_csv):
                        try:
                            with open(final_csv, 'r', encoding='utf-8') as f:
                                line_count = sum(1 for _ in f) - 1  # -1 por header
                            _csv_id_counters['with_emails'] = max(0, line_count)
                            csv_email_count = max(0, line_count)
                            logging.info(f"  - Contador CSV con emails inicializado: {line_count}")
                        except Exception:
                            pass
                    if no_emails_csv and os.path.exists(no_emails_csv):
                        try:
                            with open(no_emails_csv, 'r', encoding='utf-8') as f:
                                line_count = sum(1 for _ in f) - 1
                            _csv_id_counters['no_emails'] = max(0, line_count)
                            csv_no_email_count = max(0, line_count)
                            logging.info(f"  - Contador CSV sin emails inicializado: {line_count}")
                        except Exception:
                            pass

                    # Actualizar base_stats con valores REALES del CSV (en caso de discrepancias)
                    # results_with_email = líneas en CSV con emails (cualquier email)
                    # results_with_corporate_email = se mantiene del checkpoint (calculado correctamente por ciudad)
                    total_csv_records = csv_email_count + csv_no_email_count
                    if base_stats:
                        # Verificar si hay discrepancias y corregir
                        if base_stats.results_with_email != csv_email_count:
                            logging.info(f"  - Corrigiendo stats de emails: checkpoint={base_stats.results_with_email}, real={csv_email_count}")
                            base_stats.results_with_email = csv_email_count
                            # NO corregimos results_with_corporate_email porque eso requeriría leer el CSV completo
                        if base_stats.results_with_web != csv_no_email_count:
                            logging.info(f"  - Corrigiendo stats de webs sin email: checkpoint={base_stats.results_with_web}, real={csv_no_email_count}")
                            base_stats.results_with_web = csv_no_email_count
                        if base_stats.results_total != total_csv_records:
                            logging.info(f"  - Corrigiendo stats de totales: checkpoint={base_stats.results_total}, real={total_csv_records}")
                            base_stats.results_total = total_csv_records

                except Exception as e:
                    logging.error(f"Error cargando checkpoint ({checkpoint_file}): {e}. No se pudo retomar.")
                    config = {} # Reset config if resume fails badly
                    args.resume = False # Disable resume for interactive part
                    print("No se pudo retomar la sesión. Iniciando una nueva configuración.")

        # ---- Interactive Configuration (Only if not resuming successfully) ----
        if not resuming: 
            print("\n--- Configuración de Búsqueda ---")
            
            # Populate config from args first (for overrides)
            config_from_args = {arg: value for arg, value in vars(args).items() if value is not None and arg != 'resume'}
            config = {} # Start fresh config for interactive session
            config.update(config_from_args) # Apply CLI args
            config.setdefault('workers', DEFAULT_WORKERS) # Ensure workers default

            # 1. GeoNames Username
            if not config.get('geonames_username'):
                while not config.get('geonames_username'):
                    username = input("Introduce tu nombre de usuario de GeoNames: ").strip()
                    if username:
                        config['geonames_username'] = username
                    else:
                        print("El nombre de usuario es obligatorio.")
            # No need for else print here, will be shown in summary
            
            # 2. Country Code
            if not config.get('country_code'):
                while not config.get('country_code'):
                    code = input("Introduce el código de país (2 letras, ej: ES, GB): ").strip().upper()
                    if len(code) == 2 and code.isalpha():
                        config['country_code'] = code
                    else:
                        print("Código de país inválido. Deben ser 2 letras.")
            
            # 3. Region / Subdivision Filter (ADM1 / ADM2)
            selected_adm1_code = config.get('region_code')
            selected_adm1_name = None
            selected_adm2_code = config.get('adm2_code')
            selected_adm2_name = None

            # Helper function to select from a list
            def select_from_list(items_list, item_type_name):
                if not items_list:
                    print(f"No se encontraron {item_type_name} disponibles.")
                    return None, None

                print(f"\n{item_type_name} disponibles:")
                items_by_num = {str(i+1): item for i, item in enumerate(items_list)}
                items_by_name = {item['name'].lower(): item for item in items_list}

                for i, item in enumerate(items_list):
                    print(f"  {i+1}. {item['name']} (Código: {item.get('code', 'N/A')})")

                while True:
                    choice = input(f"Selecciona el número o nombre de {item_type_name} (o deja en blanco para 'Todas'): ").strip()
                    if not choice:
                        print(f"Buscando en todas las {item_type_name}.")
                        return None, None # No item selected

                    chosen_item = None
                    if choice.isdigit() and choice in items_by_num:
                        chosen_item = items_by_num[choice]
                    elif choice.lower() in items_by_name:
                        chosen_item = items_by_name[choice.lower()]

                    if chosen_item:
                        selected_code = chosen_item.get('code')
                        selected_name = chosen_item.get('name')
                        if selected_code:
                            print(f"Seleccionado/a {item_type_name}: {selected_name} ({selected_code})")
                            return selected_code, selected_name
                        else:
                            print(f"Error: La {item_type_name} seleccionada '{selected_name}' no tiene código. Intenta otra.")
                            # Continue loop to ask again
                    else:
                        print(f"Selección inválida: '{choice}'")

            # --- ADM1 Selection --- (Only if not provided by args)
            if selected_adm1_code is None:
                if args.batch:
                    filter_adm1 = 'n'
                    print("(Batch) Sin filtro de Región/Estado.")
                else:
                    filter_adm1 = input("¿Deseas filtrar por Región/Estado (Nivel 1)? (s/N): ").strip().lower()
                if filter_adm1 == 's':
                    regions = await get_regions_for_country(config['country_code'], config['geonames_username'])
                    selected_adm1_code, selected_adm1_name = select_from_list(regions, "Región/Estado (Nivel 1)")
            elif 'country_code' in config:
                # Try to get name for the provided adm1_code
                regions = await get_regions_for_country(config['country_code'], config['geonames_username'])
                found_region = next((r for r in regions if r.get('code') == selected_adm1_code), None)
                if found_region:
                    selected_adm1_name = found_region['name']
                else:
                    print(f"ADVERTENCIA: No se encontró el nombre para el código ADM1 '{selected_adm1_code}' proporcionado.")

            # Store selected ADM1 in config
            if selected_adm1_code:
                config['region_code'] = selected_adm1_code
                config['region_name'] = selected_adm1_name

            # --- ADM2 Selection --- (Only if ADM1 is selected and ADM2 not provided by args)
            if selected_adm1_code and selected_adm2_code is None:
                if args.batch:
                    filter_adm2 = 'n'
                    print("(Batch) Sin filtro de Provincia/Condado.")
                else:
                    filter_adm2 = input(f"¿Deseas filtrar por Provincia/Condado (Nivel 2) dentro de '{selected_adm1_name}'? (s/N): ").strip().lower()
                if filter_adm2 == 's':
                    subdivisions = await get_admin_level_2_divisions(config['country_code'], selected_adm1_code, config['geonames_username'])
                    selected_adm2_code, selected_adm2_name = select_from_list(subdivisions, "Provincia/Condado (Nivel 2)")
            elif selected_adm1_code and 'adm2_code' in config: # adm2_code provided via args
                # Try to get name for the provided adm2_code
                subdivisions = await get_admin_level_2_divisions(config['country_code'], selected_adm1_code, config['geonames_username'])
                found_subdivision = next((s for s in subdivisions if s.get('code') == selected_adm2_code), None)
                if found_subdivision:
                    selected_adm2_name = found_subdivision['name']
                else:
                     print(f"ADVERTENCIA: No se encontró el nombre para el código ADM2 '{selected_adm2_code}' proporcionado dentro de ADM1 '{selected_adm1_code}'.")

            # Store selected ADM2 in config
            if selected_adm2_code:
                 config['adm2_code'] = selected_adm2_code
                 config['adm2_name'] = selected_adm2_name

            # 4. Query
            if not config.get('query'):
                while not config.get('query'):
                    query_term = input("¿Qué quieres buscar en Google Maps? (ej: restaurantes): ").strip()
                    if query_term:
                        config['query'] = query_term
                    else:
                        print("El término de búsqueda es obligatorio.")

            # 5. Minimum Population
            if not config.get('min_population'):
                default_pop_min = 1000 # Changed default
                while config.get('min_population') is None:
                    pop_input = input(f"Población mínima de las ciudades (Enter para {default_pop_min:,}): ").strip()
                    if not pop_input:
                        config['min_population'] = default_pop_min
                        break
                    try:
                        pop = int(pop_input)
                        if pop >= 0:
                            config['min_population'] = pop
                        else:
                            print("La población debe ser un número positivo.")
                    except ValueError:
                        print("Entrada inválida. Introduce un número.")

            # 6. Maximum Population (Optional)
            if 'max_population' not in config:
                if args.batch:
                    config['max_population'] = None
                    print("(Batch) Población máxima: sin límite.")
                while 'max_population' not in config:
                    pop_input = input("Población máxima de las ciudades (Enter para 'sin límite'): ").strip()
                    if not pop_input:
                        config['max_population'] = None # Explicitly set to None for no limit
                        break
                    try:
                        pop = int(pop_input)
                        min_pop = config.get('min_population', 0)
                        if pop >= min_pop:
                            config['max_population'] = pop
                        else:
                            print(f"La población máxima debe ser mayor o igual a la mínima ({min_pop:,}).")
                    except ValueError:
                        print("Entrada inválida. Introduce un número.")
            elif config.get('max_population') is not None: # Ensure max_pop from args is valid
                 min_pop = config.get('min_population', 0)
                 if config['max_population'] < min_pop:
                     print(f"ADVERTENCIA: Población máxima ({config['max_population']:,}) proporcionada es menor que la mínima ({min_pop:,}). Se ignorará el máximo.")
                     config['max_population'] = None

            # 7. Strategy
            if not config.get('strategy'):
                default_strategy = 'simple'
                while not config.get('strategy'):
                    strategy_input = input(f"Estrategia de búsqueda [simple/grid] (Enter para '{default_strategy}'): ").strip().lower()
                    if not strategy_input:
                        config['strategy'] = default_strategy
                        break
                    if strategy_input in ['simple', 'grid']:
                        config['strategy'] = strategy_input
                    else:
                        print("Estrategia inválida. Elige 'simple' o 'grid'.")

            # 8. Max Results per Cell
            if not config.get('max_results'):
                default_max_results = 50
                while config.get('max_results') is None:
                    results_input = input(f"Máximo de resultados por celda de mapa (Enter para {default_max_results}): ").strip()
                    if not results_input:
                        config['max_results'] = default_max_results
                        break
                    try:
                        res = int(results_input)
                        if res > 0:
                            config['max_results'] = res
                        else:
                            print("El número debe ser mayor que 0.")
                    except ValueError:
                        print("Entrada inválida. Introduce un número.")

            # 9. Number of Workers (Check args first)
            if 'workers' not in config: # Ask only if not provided via args
                default_workers = DEFAULT_WORKERS
                while 'workers' not in config:
                    workers_input = input(f"Número de workers concurrentes (Enter para {default_workers}): ").strip()
                    if not workers_input:
                        config['workers'] = default_workers
                        break
                    try:
                        num_workers = int(workers_input)
                        if num_workers > 0:
                            config['workers'] = num_workers
                        else:
                            print("El número debe ser mayor que 0.")
                    except ValueError:
                        print("Entrada inválida. Introduce un número.")

            # Set resume to False as this is a new config
            config['resume'] = False

            # ---- Confirmation Step ----
            print("\n--- Resumen de Configuración ---")
            print(f"Usuario GeoNames:   {config['geonames_username']}")
            print(f"País:             {config['country_code']}")
            if config.get('region_name'):
                print(f"Región/Estado (1): {config['region_name']} ({config.get('region_code', 'N/A')})")
                if config.get('adm2_name'):
                    print(f"Provincia/Condado (2): {config['adm2_name']} ({config.get('adm2_code', 'N/A')})")
                else:
                    print(f"Provincia/Condado (2): Todas")
            else:
                print(f"Región/Estado (1): Todas")
            print(f"Búsqueda Maps:    {config['query']}")
            print(f"Población Mínima: {config['min_population']:,}")
            if config.get('max_population') is not None:
                 print(f"Población Máxima: {config['max_population']:,}")
            else:
                 print(f"Población Máxima: Sin límite")
            print(f"Estrategia:       {config['strategy']}")
            print(f"Máx. Resultados:  {config['max_results']}")
            print(f"Workers:          {config['workers']}")
            print(f"Retomar:          No")
            print("-" * 30)

            if args.batch:
                confirm = 's'
                print("(Batch) Auto-confirmando inicio de búsqueda.")
            else:
                confirm = input("¿Iniciar búsqueda con esta configuración? (S/n): ").strip().lower()
            if confirm != 's' and confirm != '' :
                print("Búsqueda cancelada.")
                return
        else:
            # Resuming: Show the loaded config
            print("\n--- Resumen de Configuración (Retomada) ---")
            print(f"Usuario GeoNames:   {config['geonames_username']}")
            print(f"País:             {config['country_code']}")
            if config.get('region_name'):
                print(f"Región/Estado (1): {config['region_name']} ({config.get('region_code', 'N/A')})")
                if config.get('adm2_name'):
                    print(f"Provincia/Condado (2): {config['adm2_name']} ({config.get('adm2_code', 'N/A')})")
                else:
                    print(f"Provincia/Condado (2): Todas")
            else:
                 print(f"Región/Estado (1): Todas")
            print(f"Búsqueda Maps:    {config['query']}")
            print(f"Población Mínima: {config['min_population']:,}")
            if config.get('max_population') is not None:
                 print(f"Población Máxima: {config['max_population']:,}")
            else:
                 print(f"Población Máxima: Sin límite")
            print(f"Estrategia:       {config['strategy']}")
            print(f"Máx. Resultados:  {config['max_results']}")
            print(f"Workers:          {config['workers']}")
            print(f"Retomar:          Sí")
            print(f"Ciudades ya hechas: {len(processed_cities)}")
            print(f"Continuando escritura en:")
            print(f"  - {final_csv}")
            print(f"  - {no_emails_csv}")
            print("-" * 30)
            if args.batch:
                confirm = 's'
                print("(Batch) Auto-confirmando continuación de búsqueda.")
            else:
                confirm = input("¿Continuar con esta búsqueda? (S/n): ").strip().lower()
            if confirm != 's' and confirm != '' :
                print("Búsqueda cancelada.")
                return

        # ---- End Configuration (Interactive or Resumed) ----

        # ---- Start Main Processing Logic ----
        print("\nIniciando proceso de scraping...")

        # Setup logging (now that config is set)
        setup_logging(dashboard_state)

        # Fetch cities using the final config
        logging.info("Obteniendo lista de ciudades desde GeoNames...")
        cities = await fetch_cities_geonames(
            country_code=config['country_code'],
            geonames_username=config['geonames_username'],
            min_population=config['min_population'],
            max_population=config.get('max_population'), # Pass max_population
            region_code=config.get('region_code'),     # Pass region_code (ADM1)
            adm2_code=config.get('adm2_code')          # Pass adm2_code
        )

        if not cities:
            logging.error("No se encontraron ciudades con los criterios especificados. Terminando.")
            return

        # Create cache directory if it doesn't exist
        cache_dir = "cache"
        os.makedirs(cache_dir, exist_ok=True)

        # Generate dynamic filenames based on final config (Only if not resuming)
        if not resuming:
            # --- Check for conflicting active runs with same config --- #
            job_manager_check = JobManager()
            active_instances = job_manager_check.list_instances()
            for inst in active_instances:
                inst_config = inst.config
                # Check if there's an active instance with the same key parameters
                if (inst_config.get('country_code') == config.get('country_code') and
                    inst_config.get('query') == config.get('query') and
                    inst_config.get('min_population') == config.get('min_population') and
                    inst_config.get('region_code') == config.get('region_code')):
                    logging.error(f"¡Ya existe un proceso activo con la misma configuración!")
                    logging.error(f"  PID: {inst.pid}, Run ID: {inst_config.get('run_id')}")
                    logging.error(f"  Query: {inst_config.get('query')} en {inst_config.get('country_code')}")
                    logging.error("Use 'scrape_manager.py' para gestionar el proceso existente o espere a que termine.")
                    return

            # --- Generate unique run_id --- #
            current_run_id = str(uuid.uuid4())[:8]  # Short UUID for readability
            logging.info(f"Nuevo Run ID generado: {current_run_id}")

            # Ensure required config keys exist
            required_keys = ['country_code', 'query', 'min_population', 'max_results', 'strategy']
            if not all(k in config for k in required_keys):
                 logging.error("Error crítico: Faltan claves de configuración para generar nombres de archivo.")
                 return

            # Store run_id in config for reference
            config['run_id'] = current_run_id

            # Use run_id for checkpoint and cache files (unique per execution)
            cache_file = os.path.join(cache_dir, f"processed_businesses_{current_run_id}.json")
            checkpoint_file = os.path.join(cache_dir, f"checkpoint_{current_run_id}.json")
            logging.info(f"Usando archivo de caché: {cache_file}")
            logging.info(f"Usando archivo de checkpoint: {checkpoint_file}")

            # Initialize files/cache for new run
            logging.info("Configurando para nueva ejecución (archivos/caché).")
            processed_cities = set()
            processed_businesses = set()
            temp_processed_businesses = set()

            # Generate descriptive filename for results (includes config info for easy identification)
            country_part = config['country_code']
            query_file_part = config['query'].replace(' ', '_').lower()[:20]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            scrappings_dir = "scrappings"
            os.makedirs(scrappings_dir, exist_ok=True)

            # Results files include both descriptive name and run_id for traceability
            final_csv = os.path.join(scrappings_dir, f"results_{country_part}_{query_file_part}_{current_run_id}_{timestamp}.csv")
            no_emails_csv = os.path.join(scrappings_dir, f"no_emails_{country_part}_{query_file_part}_{current_run_id}_{timestamp}.csv")
            logging.info(f"Creando nuevos archivos de resultados:")
            logging.info(f"  - Con correos: {final_csv}")
            logging.info(f"  - Sin correos: {no_emails_csv}")
        
        # This check should be redundant if resume logic is correct, but keep as safeguard
        elif not final_csv or not no_emails_csv or not cache_file or not checkpoint_file:
            logging.error("¡Error crítico! Retomando pero falta información de archivos (CSV/Cache/Checkpoint). Abortando.")
            return

        # Filter cities to process (those not in processed_cities set)
        cities_to_process = [city for city in cities if city['name'] not in processed_cities]

        total_cities_to_process = len(cities_to_process)
        if total_cities_to_process == 0:
            # If resuming and no new cities, the job is done
            if resuming:
                logging.info("Todas las ciudades de la configuración ya fueron procesadas en la sesión anterior.")
            else:
                # This means fetch_cities returned cities, but they were all in the (empty) processed_cities set? Unlikely.
                logging.info("No hay ciudades nuevas para procesar según los filtros aplicados.") 
                
            # Clean up checkpoint if the run is complete
            if os.path.exists(checkpoint_file):
                 try:
                     os.remove(checkpoint_file)
                     logging.info("Proceso completado para todas las ciudades. Checkpoint eliminado.")
                 except Exception as e:
                     logging.error(f"Error eliminando checkpoint final: {e}")
            return
        
        # Log the number of NEW cities to process
        logging.info(f"{total_cities_to_process} ciudades NUEVAS a procesar, ordenadas por población:")
        for city in cities_to_process[:10]: # Log first 10
            log_city_info(city["name"], city.get("population", "N/A")) # Use get for safety
        if total_cities_to_process > 10:
             logging.info("...")

        # Setup Semaphore for browser contexts
        scraper_semaphore = asyncio.Semaphore(CONCURRENT_BROWSERS)

        # --- Job Manager Setup ---
        job_manager = JobManager()

        # Create or load job
        current_job = job_manager.create_job(config)
        current_job.checkpoint_file = checkpoint_file
        current_job.csv_with_emails = final_csv
        current_job.csv_no_emails = no_emails_csv
        current_job.stats.cities_total = len(cities)
        current_job.stats.cities_processed = len(processed_cities)
        job_manager.save_job(current_job)

        # Register this instance
        pid = os.getpid()
        job_manager.register_instance(pid, current_job, config)
        logging.info(f"Job registrado: {current_job.job_id} (PID: {pid})")

        async with async_playwright() as p:
            # Launch browser once
            browser_lang_code = config.get('country_code', 'en').lower()
            browser_lang_arg = f"--lang={browser_lang_code}" # Default lang format
            # Handle specific language variations needed by Chromium if any
            if browser_lang_code == 'gb': browser_lang_arg = '--lang=en-GB'
            elif browser_lang_code == 'es': browser_lang_arg = '--lang=es-ES'
            # Add more mappings if needed: elif browser_lang_code == 'pt': browser_lang_arg = '--lang=pt-PT' # or pt-BR?

            logging.info(f"Lanzando navegador con idioma: {browser_lang_arg}")
            browser = await p.chromium.launch(
                headless=True,
                args=[browser_lang_arg]
            )

            # --- Start Control and Stats threads ---
            control_thread = ControlThread(pid, job_manager, poll_interval=1.0)
            control_thread.start()
            logging.debug("ControlThread iniciado")

            stats_thread = StatsThread(pid, current_job, job_manager, update_interval=5.0, base_stats=base_stats)
            stats_thread.stats.cities_total = len(cities)
            stats_thread.stats.cities_processed = len(processed_cities)
            stats_thread.start()
            logging.debug("StatsThread iniciado")

            # Setup dashboard if curses available
            if stdscr:
                # Set total for dashboard based on NEW cities
                dashboard_state['total_cities'] = total_cities_to_process
                # Start progress count from already processed cities if resuming
                dashboard_state['total_processed'] = len(processed_cities) if resuming else 0
                dashboard_thread = threading.Thread(target=update_dashboard, args=(stdscr, dashboard_state), daemon=True)
                dashboard_thread.start()

            # Cities Queue
            cities_queue = asyncio.Queue()
            for city in cities_to_process:
                await cities_queue.put(city) # Put the whole city dict

            # Worker function (modified for graceful shutdown + control flags)
            async def worker(worker_id: int, shutdown_event: asyncio.Event):
                logging.debug(f"Worker {worker_id}: Iniciado")
                while not shutdown_event.is_set(): # Check event before getting item
                    # --- Check control flags from ControlThread ---
                    # Check stop flag
                    if control_thread and control_thread.stop_flag:
                        logging.info(f"Worker {worker_id}: Stop solicitado via manager, finalizando...")
                        stats_thread.set_status(JobStatus.STOPPING)
                        shutdown_event.set()  # Signal all workers to stop
                        break

                    # Check pause flag - wait while paused
                    if control_thread and control_thread.pause_flag:
                        if stats_thread:
                            stats_thread.set_status(JobStatus.PAUSED)
                        logging.info(f"Worker {worker_id}: Pausado. Esperando resume...")
                        while control_thread.pause_flag and not control_thread.stop_flag:
                            await asyncio.sleep(1.0)
                        if control_thread.stop_flag:
                            logging.info(f"Worker {worker_id}: Stop recibido durante pausa.")
                            break
                        logging.info(f"Worker {worker_id}: Resumiendo...")
                        if stats_thread:
                            stats_thread.set_status(JobStatus.RUNNING)

                    city_data = None
                    try:
                        # Get item with timeout to allow checking shutdown_event periodically
                        city_data = await asyncio.wait_for(cities_queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        # Timeout is expected, just loop back to check shutdown_event
                        # If queue is empty, join() will handle termination
                        if cities_queue.empty():
                           # Optional: Check if shutdown is requested even if queue is empty
                           if shutdown_event.is_set():
                               logging.debug(f"Worker {worker_id}: Parada solicitada mientras esperaba en cola vacía.")
                               break # Exit loop if shutdown requested
                           # Otherwise, continue waiting briefly for join() signal
                        continue
                    except asyncio.CancelledError:
                        logging.debug(f"Worker {worker_id}: Tarea cancelada mientras esperaba en la cola.")
                        # No task was fully processed, just break
                        break # Exit the loop cleanly
                    except Exception as e:
                        logging.error(f"Worker {worker_id}: Error inesperado obteniendo de la cola: {e}")
                        if city_data: cities_queue.task_done() # Mark done if item was retrieved before error
                        continue # Try next item

                    city_name = city_data['name']
                    city_population = city_data.get('population', 0)
                    city_start_time = time.time()
                    logging.debug(f"Worker {worker_id}: Procesando {city_name}")

                    # Update stats with current city (para múltiples workers)
                    if stats_thread:
                        current_index = len(processed_cities) + 1
                        # Añadir a ciudades activas (soporta múltiples workers)
                        stats_thread.add_active_city(
                            city_name=city_name,
                            worker_id=worker_id,
                            population=city_population,
                            index=current_index
                        )
                        # Mantener campos legacy para compatibilidad con 1 worker
                        stats_thread.update_stats(
                            current_city=city_name,
                            current_city_index=current_index,
                            current_city_population=city_population,
                            current_city_start=datetime.now().isoformat()
                        )

                    # Update dashboard state for this city
                    if stdscr:
                        dashboard_state['active_cities'][city_name].update({
                            'status': 'Procesando',
                            'progress': 'Iniciando...',
                            'found': 0
                        })
                    try:
                        was_interrupted, retry_requested = await process_city(
                            city_data=city_data,
                            config=config, # Pass the config dict
                            browser=browser, # Pass the single browser instance
                            semaphore=scraper_semaphore, # Use the correct semaphore
                            processed_businesses=processed_businesses,
                            temp_processed_businesses=temp_processed_businesses,
                            cache_file=cache_file,
                            final_csv=final_csv,
                            no_emails_csv=no_emails_csv,
                            processed_cities=processed_cities, # Pass the set to be updated
                            checkpoint_file=checkpoint_file,
                            stats_thread=stats_thread,  # Pass stats thread for metrics
                            control_thread=control_thread  # Pass control thread for granular stop
                        )

                        # Si se solicitó retry, re-encolar la ciudad y continuar
                        if retry_requested:
                            logging.info(f"Worker {worker_id}: Re-encolando {city_name} para reintentar...")
                            if stdscr and city_name in dashboard_state['active_cities']:
                                dashboard_state['active_cities'][city_name]['status'] = 'Reintentando'
                            await cities_queue.put(city_data)  # Re-encolar
                            cities_queue.task_done()
                            continue  # Siguiente ciudad

                        # Si fue interrumpida, marcar flag y salir
                        if was_interrupted:
                            nonlocal granular_interruption
                            granular_interruption = True  # Marcar que hubo interrupción granular
                            logging.info(f"Worker {worker_id}: Ciudad {city_name} interrumpida, saliendo del worker...")
                            if stdscr and city_name in dashboard_state['active_cities']:
                                dashboard_state['active_cities'][city_name]['status'] = 'Interrumpida'
                            # Señalar a TODOS los workers que deben parar
                            shutdown_event.set()
                            cities_queue.task_done()
                            break  # Salir del worker loop

                        # Update total processed count for dashboard after successful processing
                        current_total_processed = len(processed_cities)
                        if stdscr:
                            dashboard_state['total_processed'] = current_total_processed
                            if city_name in dashboard_state['active_cities']:
                                dashboard_state['active_cities'][city_name]['status'] = 'Completada'

                        # Update stats after city completion
                        if stats_thread:
                            city_duration = time.time() - city_start_time
                            current_stats = stats_thread.get_stats()
                            # Calculate new average
                            if current_total_processed > 0:
                                # Weighted average including this city
                                total_time = current_stats.avg_city_duration * (current_total_processed - 1) + city_duration
                                new_avg = total_time / current_total_processed
                            else:
                                new_avg = city_duration
                            # Eliminar de ciudades activas
                            stats_thread.remove_active_city(city_name)
                            stats_thread.update_stats(
                                cities_processed=current_total_processed,
                                current_city="",
                                current_city_index=0,
                                current_city_population=0,
                                current_city_businesses_total=0,
                                current_city_businesses_processed=0,
                                last_city_duration=city_duration,
                                avg_city_duration=new_avg
                            )

                    except asyncio.CancelledError:
                        # Handle cancellation during process_city
                        logging.warning(f"Worker {worker_id}: Cancelado durante el procesamiento de {city_name}.")
                        # Do not mark city as processed, exit cleanly
                        # The task_done() is in finally
                        break # Exit the loop
                    except Exception as e:
                        logging.error(f"Worker {worker_id}: Error procesando {city_name}: {e}", exc_info=True)
                        if stdscr and city_name in dashboard_state['active_cities']:
                            dashboard_state['active_cities'][city_name]['status'] = 'Error'
                        # Update error count in stats
                        if stats_thread:
                            current_errors = stats_thread.stats.errors_count
                            stats_thread.update_stats(errors_count=current_errors + 1)
                        # Log error but continue processing other cities if not cancelled
                    finally:
                        # Ensure task_done is called unless cancelled before retrieval
                        if city_data: # Only call task_done if an item was actually retrieved
                           cities_queue.task_done()
                           logging.debug(f"Worker {worker_id}: Tarea completada para {city_name}. Restantes en cola: {cities_queue.qsize()}")

                logging.info(f"Worker {worker_id}: Parando.")

            # Create and start workers
            worker_tasks = [] # Reset list before populating
            num_workers = config.get('workers', DEFAULT_WORKERS)
            logging.info(f"Iniciando procesamiento con {num_workers} worker{'s' if num_workers > 1 else ''}")
            for i in range(num_workers):
                task = asyncio.create_task(worker(i + 1, shutdown_event), name=f"Worker-{i+1}")
                worker_tasks.append(task)

            # Wait for queue to finish OR shutdown signal
            # We use asyncio.wait for more control than queue.join()
            monitor_task = asyncio.create_task(cities_queue.join(), name="QueueMonitor")
            shutdown_wait_task = asyncio.create_task(shutdown_event.wait(), name="ShutdownMonitor")

            done, pending = await asyncio.wait(
                [monitor_task, shutdown_wait_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Track if queue completed normally (before shutdown was triggered)
            queue_completed_normally = False

            if shutdown_wait_task in done:
                logging.info("Señal de parada recibida, cancelando tareas pendientes...")
                # Cancel the queue monitor if it's still pending
                if monitor_task in pending:
                    monitor_task.cancel()
                # Proceed to cancel workers (which should happen shortly anyway)
            else:
                logging.info("Todas las ciudades de la cola han sido procesadas.")
                queue_completed_normally = True  # Mark as completed normally
                # Cancel the shutdown monitor as it's no longer needed
                if shutdown_wait_task in pending:
                    shutdown_wait_task.cancel()

            # --- Graceful Worker Shutdown --- #
            if worker_tasks:
                 logging.info("Esperando finalización elegante de los workers...")
                 # Signal shutdown if not already done (e.g., if queue finished first)
                 if not shutdown_event.is_set():
                     shutdown_event.set()

                 # Cancel any workers that might still be running (e.g., waiting for timeout)
                 for task in worker_tasks:
                     if not task.done():
                         task.cancel()

                 # Wait for all worker tasks to complete cancellation/exit
                 results = await asyncio.gather(*worker_tasks, return_exceptions=True)
                 logging.info("Workers finalizados.")
                 # Log any unexpected errors from workers during shutdown
                 for i, res in enumerate(results):
                     # Don't log CancelledError as an error here
                     if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                         logging.error(f"Worker {i+1} reportó un error durante la parada: {res}", exc_info=isinstance(res, Exception))

    except KeyboardInterrupt:
        # This block now primarily handles saving the checkpoint after shutdown signal
        logging.info("\n\nInterrupción de teclado (Ctrl+C) detectada en main.")
        # Ensure shutdown is signalled if the handler didn't catch it
        if not shutdown_event.is_set():
             logging.warning("Señalando parada desde el bloque KeyboardInterrupt (fallback).")
             shutdown_event.set()
        # *** Crucial: Wait for workers to finish cancellation if they haven't already ***
        # This check is important if Ctrl+C happens very quickly
        if worker_tasks and not all(t.done() for t in worker_tasks):
            logging.info("Esperando finalización de workers después de Ctrl+C...")
            # Ensure they are cancelled
            for task in worker_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True) # Wait here
            logging.info("Workers finalizados después de Ctrl+C.")

        # --- Save Checkpoint --- #
        # Save checkpoint only if config and file paths are available AND workers finished
        if 'checkpoint_file' in locals() and checkpoint_file and config and 'processed_cities' in locals() and 'final_csv' in locals() and final_csv and 'no_emails_csv' in locals() and no_emails_csv:
             try:
                 # processed_cities should now be accurate as workers finished cleanly
                 logging.info(f"Guardando checkpoint en {checkpoint_file}... ({len(processed_cities)} ciudades completadas)")

                 # Obtener stats actuales para guardarlas
                 accumulated_stats = None
                 if 'stats_thread' in locals() and stats_thread:
                     current_stats = stats_thread.get_stats()
                     accumulated_stats = {
                         'runtime_seconds': current_stats.runtime_seconds,
                         'results_total': current_stats.results_total,
                         'results_with_email': current_stats.results_with_email,
                         'results_with_corporate_email': current_stats.results_with_corporate_email,
                         'results_with_web': current_stats.results_with_web,
                         'errors_count': current_stats.errors_count,
                         'avg_city_duration': current_stats.avg_city_duration,
                     }

                 with open(checkpoint_file, 'w', encoding='utf-8') as f:
                     checkpoint_data = {
                         'run_id': config.get('run_id'),
                         'processed_cities': sorted(list(processed_cities)), # Save sorted list
                         'last_update': datetime.now().isoformat(),
                         'final_csv': final_csv,
                         'no_emails_csv': no_emails_csv,
                         'config': config, # Save the config used
                         'accumulated_stats': accumulated_stats
                     }
                     json.dump(checkpoint_data, f, indent=4)
                 logging.info(f"Checkpoint guardado. El proceso se puede retomar usando --resume --run-id {config.get('run_id')}")
             except Exception as e:
                 logging.error(f"Error guardando checkpoint: {e}")
        else:
            logging.warning("No se pudo guardar el checkpoint (faltan datos o ocurrió antes de la inicialización/finalización de workers).")
        # No return here, let it proceed to finally

    except Exception as e:
        logging.error(f"Error inesperado en main: {e}", exc_info=True)
        # Signal shutdown on unexpected error too
        if not shutdown_event.is_set():
            shutdown_event.set()
        # Attempt to wait for workers even on general errors
        if worker_tasks:
             logging.info("Intentando parada elegante de workers después de error inesperado...")
             for task in worker_tasks:
                 if not task.done():
                     task.cancel()
             await asyncio.gather(*worker_tasks, return_exceptions=True)

    finally:
        # --- Final Cleanup --- #
        # OPTIMIZACIÓN v2: Cerrar sesión aiohttp compartida
        try:
            await close_shared_session()
            logging.debug("Sesión aiohttp compartida cerrada")
        except Exception as e:
            logging.debug(f"Error cerrando sesión aiohttp: {e}")

        # Stop control and stats threads
        if control_thread:
            control_thread.stop()
            logging.debug("ControlThread detenido")
        if stats_thread:
            stats_thread.stop()
            logging.debug("StatsThread detenido")

        # Ensure dashboard stops if it was started
        if 'dashboard_state' in globals():
            dashboard_state['running'] = False
        if dashboard_thread and dashboard_thread.is_alive():
            logging.debug("Esperando finalización del dashboard...")
            dashboard_thread.join(timeout=1)

        # Close browser ONLY after workers have finished (or attempted shutdown)
        if browser and browser.is_connected():
            logging.info("Cerrando navegador...")
            try:
                await browser.close()
                logging.info("Navegador cerrado.")
            except Exception as e:
                 logging.error(f"Error al cerrar el navegador en finally: {e}")
        else:
            logging.debug("Navegador no iniciado o ya cerrado.")

        # Determine final job status and update
        final_status = JobStatus.INTERRUPTED
        # Check if queue completed normally (set before shutdown_event was triggered for worker cleanup)
        # IMPORTANTE: Si hubo interrupción granular (a mitad de ciudad), siempre es INTERRUPTED
        if granular_interruption:
            final_status = JobStatus.INTERRUPTED
            logging.info("Proceso interrumpido a mitad de ciudad (parada granular).")
        elif 'queue_completed_normally' in locals() and queue_completed_normally:
            final_status = JobStatus.COMPLETED
            logging.info("Proceso completado normalmente.")
            # Completed successfully, remove checkpoint if it exists
            if 'checkpoint_file' in locals() and checkpoint_file and os.path.exists(checkpoint_file):
                try:
                    os.remove(checkpoint_file)
                    logging.info("Checkpoint eliminado tras finalización normal.")
                except Exception as e:
                    logging.error(f"Error eliminando checkpoint final: {e}")
        elif shutdown_event.is_set():
             logging.info("Proceso interrumpido.")
        else: # Probably an error happened before queue processing finished
             final_status = JobStatus.FAILED
             logging.warning("El proceso terminó, pero el estado de finalización es incierto (posible error temprano).")

        # Update job with final status
        if job_manager and current_job:
            try:
                current_job.status = final_status
                current_job.finished_at = datetime.now().isoformat()
                if stats_thread:
                    current_job.stats = stats_thread.get_stats()
                job_manager.save_job(current_job)
                job_manager.unregister_instance(os.getpid())
                logging.info(f"Job {current_job.job_id} finalizado con estado: {final_status}")
            except Exception as e:
                logging.error(f"Error actualizando job final: {e}")


if __name__ == "__main__":
    # Basic logging setup initially
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s',
        datefmt='%H:%M:%S'
    )

    # Run without curses for simplicity during development
    asyncio.run(main(None)) 