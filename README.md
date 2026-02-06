# Google Maps Scraper con GeoNames

Scraper de Google Maps que extrae negocios (nombre, telefono, direccion, rating, web y emails corporativos) utilizando la API de GeoNames para segmentar busquedas por ciudades.

## Requisitos

- Python 3.11+ (recomendado 3.13)
- [uv](https://github.com/astral-sh/uv) (recomendado) o pip
- Google Chrome/Chromium (lo instala Playwright automaticamente)

## Instalacion

### macOS

```bash
# Instalar uv si no lo tienes
brew install uv

# Clonar el repositorio
git clone https://github.com/gorkavidal/scrapping.git
cd scrapping

# Crear entorno virtual e instalar dependencias
uv venv
uv pip install -r requirements.txt

# Instalar navegador Chromium para Playwright
.venv/bin/python -m playwright install chromium
```

### Linux (Ubuntu/Debian)

```bash
# Instalar dependencias del sistema
sudo apt update
sudo apt install -y python3 python3-venv curl

# Instalar uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clonar el repositorio
git clone https://github.com/gorkavidal/scrapping.git
cd scrapping

# Crear entorno virtual e instalar dependencias
uv venv
uv pip install -r requirements.txt

# Instalar Chromium y sus dependencias del sistema
.venv/bin/python -m playwright install chromium
.venv/bin/python -m playwright install-deps chromium
```

> En Linux el comando `install-deps` es necesario para instalar las librerias del sistema que Chromium necesita (libgbm, libasound2, etc.).

### Windows

```powershell
# Instalar uv (PowerShell como Administrador)
irm https://astral.sh/uv/install.ps1 | iex

# Clonar el repositorio
git clone https://github.com/gorkavidal/scrapping.git
cd scrapping

# Crear entorno virtual e instalar dependencias
uv venv
.venv\Scripts\activate
uv pip install -r requirements.txt

# Instalar navegador Chromium para Playwright
python -m playwright install chromium
```

> En Windows, los comandos se ejecutan con `.venv\Scripts\python` en lugar de `.venv/bin/python`. El flag `--continue-run` lanza el proceso en background, pero si cierras la terminal de Windows el proceso muere. Para ejecucion prolongada en Windows usa `start /B` o ejecutalo dentro de una sesion de `tmux` en WSL.

### Docker / Dev Container

El repositorio incluye una configuracion de Dev Container en `.devcontainer/devcontainer.json` basada en la imagen oficial de Playwright:

```bash
# Si usas VS Code con la extension Dev Containers
# Simplemente abre el proyecto y selecciona "Reopen in Container"
# Las dependencias se instalan automaticamente
```

### Instalacion sin uv (con pip clasico)

Si prefieres no usar `uv`:

```bash
python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

pip install -r requirements.txt
python -m playwright install chromium
```

## Inicio rapido

El flujo recomendado tiene dos pasos: un setup inicial (una sola vez) y luego lanzar el scraping en segundo plano tantas veces como quieras.

> En todos los ejemplos se usa `.venv/bin/python` (Linux/macOS). En Windows usa `.venv\Scripts\python` en su lugar.

### Paso 1: Setup inicial (interactivo)

```bash
.venv/bin/python scrape_maps_interactive.py --setup
```

Este comando:

1. Te pide los parametros de busqueda de forma interactiva (pais, query, poblacion, etc.)
2. Abre un navegador visible con Google Maps
3. Tu aceptas las cookies y verificaciones de Google manualmente
4. Cierras el navegador cuando veas los resultados en el mapa
5. Se guardan tanto la configuracion como las cookies del navegador

### Paso 2: Lanzar en segundo plano

```bash
.venv/bin/python scrape_maps_interactive.py --continue-run
```

Este comando:

1. Lee la configuracion que guardaste en el setup
2. Muestra un resumen y lanza el scraping como proceso de fondo
3. Te da el PID y la ruta del log
4. Puedes cerrar la terminal, el scraping sigue corriendo

Para ver el progreso:

```bash
tail -f scrapping_background.log
```

## Modos de ejecucion

### Modo interactivo (por defecto)

```bash
.venv/bin/python scrape_maps_interactive.py
```

El script te hace todas las preguntas paso a paso por terminal. Util para una ejecucion puntual donde puedes dejar la terminal abierta.

### Modo setup + continue (recomendado para background)

```bash
# Solo la primera vez (o cuando quieras cambiar parametros)
.venv/bin/python scrape_maps_interactive.py --setup

# Cada vez que quieras lanzar el scraping
.venv/bin/python scrape_maps_interactive.py --continue-run
```

### Modo batch (todo por linea de comandos)

```bash
.venv/bin/python scrape_maps_interactive.py \
    --batch \
    --country_code "ES" \
    --query "restaurantes" \
    --min_population 5000 \
    --strategy "simple" \
    --max_results 50 \
    --workers 2
```

Con `--batch` no se hace ninguna pregunta interactiva. Los parametros no especificados usan valores por defecto. Puedes combinarlo con `nohup` para segundo plano:

```bash
nohup .venv/bin/python scrape_maps_interactive.py \
    --batch --country_code "ES" --query "hoteles" \
    --min_population 50000 --strategy "simple" \
    > scrapping.log 2>&1 &
```

### Modo resume (retomar ejecucion interrumpida)

```bash
# Retomar el ultimo checkpoint
.venv/bin/python scrape_maps_interactive.py --resume

# Retomar un run especifico por su ID
.venv/bin/python scrape_maps_interactive.py --resume --run-id abc12345
```

Carga el checkpoint y continua desde donde se quedo. Cada ejecucion tiene un `run_id` unico (8 caracteres) que se muestra al iniciar y en los logs. Si no se especifica `--run-id`, retoma el checkpoint mas reciente.

## Parametros

### Modos de operacion

| Parametro | Descripcion |
|-----------|-------------|
| `--setup` | Configuracion interactiva + verificacion de Google Maps en navegador visible. Guarda config y cookies para uso posterior. |
| `--continue-run` | Lanza el scraping en segundo plano usando la configuracion guardada por `--setup`. |
| `--batch` | Modo no interactivo. No hace preguntas, usa valores por defecto para lo no especificado. |
| `--resume` | Retoma la ejecucion desde el ultimo checkpoint guardado. |
| `--run-id ID` | (Con `--resume`) Especifica el run_id exacto a retomar en lugar del mas reciente. |

### Parametros de busqueda

| Parametro | Tipo | Default | Descripcion |
|-----------|------|---------|-------------|
| `--geonames_username` | str | `gorkota` | Nombre de usuario de GeoNames API. Tambien acepta la variable de entorno `GEONAMES_USERNAME`. |
| `--country_code` | str | *(pregunta)* | Codigo de pais ISO 3166-1 alpha-2 (ej: `ES`, `GB`, `FR`, `DE`). |
| `--region_code` | str | *(ninguno)* | Codigo de region/comunidad autonoma (ADM1). Filtra ciudades por esta region. |
| `--adm2_code` | str | *(ninguno)* | Codigo de provincia/condado (ADM2). Requiere `--region_code`. |
| `--query` | str | *(pregunta)* | Termino de busqueda en Google Maps (ej: `restaurantes`, `hoteles`, `dentistas`). |
| `--min_population` | int | `1000` | Poblacion minima de las ciudades a procesar. |
| `--max_population` | int | *(sin limite)* | Poblacion maxima de las ciudades a procesar. |

### Parametros de scraping

| Parametro | Tipo | Default | Descripcion |
|-----------|------|---------|-------------|
| `--strategy` | str | `simple` | Estrategia de cobertura geografica: `simple` (1 celda por ciudad) o `grid` (malla de celdas, mas cobertura). |
| `--max_results` | int | `50` | Maximo de resultados a extraer por celda de mapa. |
| `--workers` | int | `1` | Numero de workers concurrentes procesando ciudades en paralelo. |

## Estrategias de busqueda

### Simple

Una unica celda centrada en la ciudad. Mas rapida pero puede perder negocios en ciudades grandes.

- Ciudades > 500k hab: celda de 2.5km, zoom 11
- Ciudades 40k-500k hab: celda de 2km, zoom 12
- Ciudades < 40k hab: celda de 1.5km, zoom 12

### Grid

Divide la ciudad en una malla de celdas para cobertura mas completa. Mas lenta pero mas exhaustiva.

- Ciudades > 500k hab: malla 3x3 (9 celdas), zoom 15
- Ciudades 40k-500k hab: malla 2x2 (4 celdas), zoom 15
- Ciudades < 40k hab: 1 celda, zoom 15

## Archivos de salida

### CSVs de resultados

Se generan en la carpeta `scrappings/`:

- `results_*.csv` - Negocios que tienen email (corporativo o generico)
- `no_emails_*.csv` - Negocios con web pero sin emails encontrados

Columnas:

| Columna | Descripcion |
|---------|-------------|
| Name | Nombre del negocio |
| Localidad | Ciudad donde se encontro |
| Region | Region/comunidad autonoma |
| Address | Direccion postal |
| Phone | Telefono |
| Rating | Valoracion en Google Maps |
| Website | URL de la web del negocio |
| Email_Raw | Todos los emails encontrados (separados por `;`) |
| Email_Filtered | Solo emails corporativos (dominio coincide con el del negocio) |
| Email_Search_Status | Estado de la busqueda de email |
| ID | Hash MD5 unico del negocio |

### Checkpoints y Run IDs

Cada ejecucion tiene un **run_id** unico (8 caracteres, ej: `a1b2c3d4`) que identifica de forma inequivoca esa sesion de scraping. Esto permite:

- Retomar una ejecucion especifica con `--resume --run-id abc12345`
- Evitar conflictos si lanzas accidentalmente dos procesos con la misma configuracion
- Trazabilidad entre archivos de resultados y checkpoints

Los archivos se guardan en la carpeta `cache/`:

- `checkpoint_{run_id}.json` - Estado de la ejecucion para poder retomar con `--resume`
- `processed_businesses_{run_id}.json` - Cache de negocios ya procesados (evita duplicados)

Los archivos de resultados incluyen el run_id en el nombre:
- `results_{pais}_{query}_{run_id}_{timestamp}.csv`

### Configuracion del setup

Se guarda en `browser_data/`:

- `setup_config.json` - Parametros de busqueda del ultimo `--setup`
- `google_maps_state.json` - Cookies y estado del navegador para Google Maps

## Constantes internas

Estas constantes se definen al inicio del script y controlan el comportamiento del scraping:

| Constante | Valor | Descripcion |
|-----------|-------|-------------|
| `CONCURRENT_BROWSERS` | 8 | Contextos de navegador simultaneos max. |
| `MAX_CONCURRENT_EXTRACTIONS` | 20 | Extracciones de email simultaneas max. |
| `MAX_CONTACT_LINKS` | 3 | Paginas de contacto a revisar por web |
| `MAX_TOTAL_TIME` | 30s | Tiempo maximo por URL al extraer emails |
| `MAIN_PAGE_TIMEOUT` | 15000ms | Timeout para cargar la pagina principal de un negocio |
| `CONTACT_PAGE_TIMEOUT` | 10000ms | Timeout para cargar paginas de contacto |
| `NETWORKIDLE_TIMEOUT` | 5000ms | Timeout para esperar network idle |

## Ejemplos de uso

### Buscar hoteles en toda Espana (ciudades > 100k hab)

```bash
.venv/bin/python scrape_maps_interactive.py --setup
# Configurar: ES, hoteles, 100000, simple
# Aceptar cookies en el navegador, cerrar

.venv/bin/python scrape_maps_interactive.py --continue-run
```

### Buscar dentistas en Cataluna (ciudades > 5k hab)

```bash
.venv/bin/python scrape_maps_interactive.py \
    --batch \
    --country_code "ES" \
    --region_code "56" \
    --query "dentistas" \
    --min_population 5000 \
    --strategy "grid" \
    --max_results 100 \
    --workers 3
```

### Buscar restaurantes en Reino Unido con rango de poblacion

```bash
.venv/bin/python scrape_maps_interactive.py \
    --batch \
    --country_code "GB" \
    --query "restaurants" \
    --min_population 20000 \
    --max_population 200000 \
    --strategy "simple" \
    --max_results 50
```

## Gestor de instancias (scrape_manager.py)

El gestor de instancias proporciona una interfaz interactiva tipo `htop` para controlar los procesos de scraping en ejecucion. Permite monitorizar, pausar, reanudar, parar y modificar workers de las instancias activas en tiempo real.

### Lanzar el gestor

```bash
.venv/bin/python scrape_manager.py
```

### Vistas disponibles

El gestor tiene tres vistas principales:

| Vista | Tecla | Descripcion |
|-------|-------|-------------|
| Instancias activas | `1` | Procesos de scraping en ejecucion con metricas en tiempo real |
| Historico de jobs | `2` | Todos los jobs ejecutados (completados, interrumpidos, fallidos) |
| Visor de archivos | `F` | Vista tabular del CSV de resultados de la instancia seleccionada |

### Controles globales

| Tecla | Accion | Descripcion |
|-------|--------|-------------|
| `N` | Nuevo | Abre el asistente para lanzar un nuevo scraping. |
| `1` | Vista activas | Cambia a la vista de instancias activas. |
| `2` | Vista historico | Cambia a la vista de historico de jobs. |
| `Q` | Salir | Cierra el gestor (los procesos siguen corriendo). |

### Controles en vista de instancias activas

| Tecla | Accion | Descripcion |
|-------|--------|-------------|
| `P` | Pausar | Pausa la instancia seleccionada. El proceso espera sin consumir recursos. |
| `R` | Reanudar | Reanuda una instancia pausada. |
| `S` | Stop graceful | Envia senal de parada. El proceso termina la ciudad actual y para. |
| `K` | Kill | Termina el proceso inmediatamente con SIGKILL. Util para procesos pausados. |
| `W` | Workers | Cambia el numero de workers. Para el proceso y lo relanza con la nueva configuracion. |
| `F` | Files | Abre el visor de archivos con los resultados de la instancia seleccionada. |
| `↑/↓` o `j/k` | Navegar | Mueve la seleccion entre instancias. |

### Controles en vista de historico

| Tecla | Accion | Descripcion |
|-------|--------|-------------|
| `Enter` | Revivir | Relanza un job interrumpido o fallido usando `--resume`. |
| `D` | Eliminar | Elimina el job del historico (no afecta archivos de resultados). |
| `F` | Files | Abre el visor de archivos con los resultados del job seleccionado. |

### Controles en visor de archivos

| Tecla | Accion | Descripcion |
|-------|--------|-------------|
| `↑/↓` o `j/k` | Scroll | Navega por los registros del CSV. |
| `PgUp/PgDn` | Scroll rapido | Salta 20 registros arriba/abajo. |
| `E` | Toggle emails | Alterna entre mostrar emails corporativos o todos los emails. |

### Columnas en vista de instancias activas

| Columna | Descripcion |
|---------|-------------|
| PID | ID del proceso del sistema operativo |
| Estado | Estado actual: `running`, `paused`, `stopping`, o comando pendiente (`PAUSING...`, etc.) |
| W | Numero de workers configurados |
| Cities | Ciudades procesadas / total |
| Total | Total de resultados extraidos |
| Emails | Resultados con email encontrado |
| Runtime | Tiempo de ejecucion |
| Ciudad Actual | Ciudad que se esta procesando actualmente |

### Panel de detalles

Debajo de la lista de instancias se muestra informacion detallada de la instancia seleccionada:

- **Job ID**: Identificador unico del trabajo
- **Query**: Termino de busqueda configurado
- **Pais**: Codigo de pais
- **Poblacion minima**: Filtro de poblacion
- **Estrategia**: `simple` o `grid`
- **Max resultados**: Limite de resultados por celda
- **Con email corporativo**: Emails que coinciden con el dominio del negocio
- **Con web**: Negocios con pagina web
- **Errores**: Numero de errores durante la ejecucion
- **ETA**: Tiempo estimado restante basado en la media por ciudad

### Cambio dinamico de workers

Al pulsar `W` para cambiar el numero de workers:

1. **Si el proceso esta pausado**: Se hace kill directo (el checkpoint ya esta guardado) y se relanza inmediatamente con los nuevos workers.

2. **Si el proceso esta en ejecucion**: Se envia STOP y se espera a que termine la ciudad actual. Durante la espera se muestra un contador y puedes:
   - Pulsar `F` para forzar kill inmediato
   - Pulsar `Esc` para cancelar y dejar el proceso corriendo

El proceso se relanza automaticamente con `--resume` usando el nuevo numero de workers.

### Persistencia de comandos

Los estados de comando (`PAUSING...`, `STOPPING...`, etc.) se muestran en la columna Estado hasta que el proceso confirma el cambio de estado. Esto proporciona feedback visual de que el comando fue enviado y esta pendiente de ejecucion.

### Latencia de comandos

Los comandos (pause, resume, stop) se envian via archivos en `cache/control/` y son leidos por el scraper cada ~1 segundo. Los cambios de estado se reflejan en la siguiente iteracion del worker.

### Limpieza automatica

El gestor detecta automaticamente procesos muertos (zombies) y los limpia del registro de instancias activas, marcando sus jobs como `interrupted` en el historico.

### Lanzar nuevo scraping (tecla N)

Al pulsar `N` se abre un asistente interactivo para configurar y lanzar un nuevo scraping:

1. **Verificacion de cookies**: El gestor comprueba si existen cookies validas de Google Maps (menos de 7 dias de antiguedad).
   - Si no hay cookies validas, ofrece ejecutar `--setup` para obtenerlas.
   - Si el usuario elige continuar sin cookies, el scraping probablemente fallara.

2. **Formulario de configuracion**: Campos editables con valores por defecto:
   - Pais (codigo de 2 letras)
   - Termino de busqueda
   - Poblacion minima/maxima
   - Workers paralelos (1-10)
   - Estrategia (simple/grid)
   - Max resultados por celda

3. **Lanzamiento**: El scraping se lanza en segundo plano con `--batch` y los logs se guardan en `scrapping_background.log`.

### Estadisticas persistentes

Las estadisticas de cada run (tiempo de ejecucion, resultados, etc.) se mantienen entre pausas y resumes:

- Al pausar o interrumpir, las stats se guardan en el checkpoint
- Al retomar con `--resume`, las stats anteriores se recuperan y se suman a las nuevas
- El tiempo de ejecucion mostrado es el tiempo real acumulado de scraping activo (no incluye pausas)

## Notas importantes

- **Primera ejecucion**: Siempre ejecuta `--setup` primero para aceptar cookies/verificaciones de Google Maps. Sin este paso, el scraping en modo headless no obtendra resultados.
- **Cookies caducadas**: Si el scraping empieza a devolver 0 resultados, vuelve a ejecutar `--setup` para renovar las cookies.
- **Rate limiting**: Google Maps puede limitar las peticiones si se hacen demasiadas en poco tiempo. Usar pocos workers (1-2) es mas seguro.
- **Deduplicacion**: El script deduplica automaticamente por nombre+direccion+telefono y por web+telefono. No genera duplicados aunque las celdas se solapen.
- **Proteccion contra conflictos**: Si intentas lanzar una nueva ejecucion con la misma configuracion que un proceso activo, el script lo detecta y aborta para evitar corrupcion de datos.
- **Interrupcion segura**: Puedes parar el proceso con `Ctrl+C` o `kill`. El checkpoint se guarda automaticamente y puedes retomar con `--resume --run-id {id}`.
- **Gestor de instancias**: Usa `scrape_manager.py` para controlar instancias en ejecucion sin interrumpir el scraping.
- **GeoNames API**: Necesitas una cuenta gratuita en [geonames.org](https://www.geonames.org/login) para usar la API. El usuario por defecto es `gorkota`.
