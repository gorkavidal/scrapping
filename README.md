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
| `--geonames_username` | str | `gorkota` | Nombre de usuario de GeoNames API. Tambien acepta la variable de entorno `GEONAMES_USERNAME`. Si experimentas errores de limite, [crea tu propia cuenta gratis](https://www.geonames.org/login). |
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
| `S` | Stop graceful | Envia senal de parada. El proceso termina el batch actual (cada 10 negocios) y para. |
| `K` | Kill | Termina el proceso inmediatamente con SIGKILL. Util para procesos pausados. |
| `W` | Workers | Cambia el numero de workers. Para el proceso y lo relanza con la nueva configuracion. |
| `T` | Retry | Reinicia una ciudad atascada. Util cuando la busqueda en Maps no avanza. |
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
| Ciudades activas | Ciudades en proceso (una por worker activo) |

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

#### Progreso de ciudades activas

Cuando hay workers procesando ciudades, el panel muestra el estado de cada una:

- **Buscando en Maps**: Indicador animado mientras se buscan negocios en Google Maps
- **Barra de progreso**: Una vez encontrados los negocios, muestra `[████░░░░░░] 45/100` con el progreso de extraccion de emails

Si usas multiples workers, cada ciudad activa se muestra con su worker asignado y progreso individual.

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

Los comandos (pause, resume, stop) se envian via archivos en `cache/control/` y son leidos por el scraper despues de cada batch de 10 negocios. Esto permite una respuesta granular: no tienes que esperar a que termine toda la ciudad, solo el batch actual.

### Retry de ciudades atascadas

Si una ciudad parece atascada (el indicador "Buscando en Maps..." no avanza durante mucho tiempo), puedes reiniciar su procesamiento:

1. Pulsa `T` para activar el modo retry
2. Si hay multiples ciudades activas, selecciona cual reiniciar
3. El worker abandona la busqueda actual y vuelve a encolar la ciudad

El retry solo afecta a la busqueda en Maps. Si la ciudad ya estaba extrayendo emails, el progreso se pierde para esa ciudad (las demas no se ven afectadas).

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

### Parada granular y checkpoints

El sistema responde a comandos PAUSE y STOP de forma granular:

- **Checkpoint inmediato tras busqueda**: Cuando se encuentran negocios en Maps, se guarda checkpoint inmediatamente, antes de empezar la extraccion de emails. Esto evita perder el trabajo de busqueda si se para el proceso.
- **Respuesta por batch**: Los comandos se procesan cada 10 negocios (un batch). No tienes que esperar a que termine toda la ciudad.
- **Estado correcto al parar**: Si paras durante el procesamiento, el job queda marcado como `interrupted` (no `completed`), permitiendo retomarlo con `--resume`.

### Guardado incremental de resultados

Los resultados se guardan al CSV **inmediatamente despues de cada batch** (cada 10 negocios procesados), no al final de la ciudad:

- **Sin perdida de datos**: Si se interrumpe el proceso, los batches ya guardados no se pierden.
- **Resume seguro**: Al retomar con `--resume`, los negocios ya guardados en el CSV no se vuelven a procesar.
- **Stats en tiempo real**: Las estadisticas se actualizan despues de cada batch guardado.

### Estadisticas de emails

Las estadisticas distinguen entre tipos de email:

- **Emails**: Negocios con cualquier email encontrado (raw o corporativo). Coincide exactamente con las filas del CSV `results_*.csv`.
- **Corporativos**: Negocios cuyo email coincide con el dominio de su web (ej: `info@empresa.com` para `empresa.com`).
- **Solo web**: Negocios con pagina web pero sin email encontrado.

Al hacer `--resume`, las stats se sincronizan automaticamente con el contenido real de los archivos CSV para evitar discrepancias.

## Notas importantes

- **Primera ejecucion**: Siempre ejecuta `--setup` primero para aceptar cookies/verificaciones de Google Maps. Sin este paso, el scraping en modo headless no obtendra resultados.
- **Cookies caducadas**: Si el scraping empieza a devolver 0 resultados, vuelve a ejecutar `--setup` para renovar las cookies.
- **Rate limiting**: Google Maps puede limitar las peticiones si se hacen demasiadas en poco tiempo. Usar pocos workers (1-2) es mas seguro.
- **Deduplicacion**: El script deduplica automaticamente por nombre+direccion+telefono y por web+telefono. No genera duplicados aunque las celdas se solapen.
- **Proteccion contra conflictos**: Si intentas lanzar una nueva ejecucion con la misma configuracion que un proceso activo, el script lo detecta y aborta para evitar corrupcion de datos.
- **Interrupcion segura**: Puedes parar el proceso con `Ctrl+C`, `kill`, o desde el gestor con `S`. El proceso para despues del batch actual (cada 10 negocios), guarda checkpoint y puedes retomar con `--resume --run-id {id}`.
- **Gestor de instancias**: Usa `scrape_manager.py` para controlar instancias en ejecucion sin interrumpir el scraping.
- **GeoNames API**: El script usa `gorkota` como usuario por defecto. Si experimentas errores de limite (rate limiting), [crea tu propia cuenta gratis](https://www.geonames.org/login), [activa el servicio web](https://www.geonames.org/manageaccount), y configura tu usuario con `export GEONAMES_USERNAME=tu_usuario` o pasalo con `--geonames_username`.

## Skill para Claude Code / AI Assistants

Este repositorio incluye un **skill** que permite a asistentes de IA (Claude Code, Codex, etc.) controlar el scraper de forma conversacional. El usuario puede decir cosas como:

- "¿Como va el scraping?"
- "Busca clinicas dentales en Francia"
- "Pausa el scraping y muestrame los resultados"

### Estructura del Skill

```
skill/
├── SKILL.md                # Definicion del skill (instrucciones para el AI)
└── scripts/
    ├── scraper_status.py   # Status rapido (ligero, para uso frecuente)
    └── scraper_cli.py      # CLI completo (start/pause/resume/stop/results)
```

### Instalacion del Skill

#### Opcion 1: Instalacion local (recomendada)

Esta opcion mantiene el skill junto al scraper, facilitando actualizaciones.

```bash
# 1. Clonar el repositorio en una ubicacion permanente
git clone https://github.com/gorkavidal/scrapping.git ~/tools/scrapping
cd ~/tools/scrapping

# 2. Instalar dependencias
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium

# 3. Configurar variable de entorno (añadir a ~/.bashrc o ~/.zshrc)
echo 'export SCRAPER_PATH="$HOME/tools/scrapping"' >> ~/.zshrc
source ~/.zshrc

# 4. Crear enlace simbolico al skill en el directorio de skills
mkdir -p ~/.agents/skills
ln -s ~/tools/scrapping/skill ~/.agents/skills/gmaps-scraper

# 5. (Opcional) Registrar en .skill-lock.json para que Claude Code lo reconozca
```

#### Opcion 2: Instalacion standalone del skill

Si quieres instalar solo el skill sin clonar todo el repo:

```bash
# Crear directorio de skills si no existe
mkdir -p ~/.agents/skills/gmaps-scraper/scripts

# Descargar archivos del skill
curl -o ~/.agents/skills/gmaps-scraper/SKILL.md \
  https://raw.githubusercontent.com/gorkavidal/scrapping/main/skill/SKILL.md
curl -o ~/.agents/skills/gmaps-scraper/scripts/scraper_status.py \
  https://raw.githubusercontent.com/gorkavidal/scrapping/main/skill/scripts/scraper_status.py
curl -o ~/.agents/skills/gmaps-scraper/scripts/scraper_cli.py \
  https://raw.githubusercontent.com/gorkavidal/scrapping/main/skill/scripts/scraper_cli.py

# Hacer ejecutables
chmod +x ~/.agents/skills/gmaps-scraper/scripts/*.py
```

**Nota**: Esta opcion requiere que el scraper este instalado por separado y que `SCRAPER_PATH` apunte a el.

#### Opcion 3: Instalacion en otros ordenadores

Para instalar en un nuevo ordenador:

```bash
# === PASO 1: Instalar el scraper ===
# Elegir ubicacion (recomendado: ~/tools o /opt para uso compartido)
INSTALL_DIR="$HOME/tools/scrapping"
git clone https://github.com/gorkavidal/scrapping.git "$INSTALL_DIR"
cd "$INSTALL_DIR"

# Crear entorno virtual
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium

# === PASO 2: Configurar variable de entorno ===
# Añadir a tu shell config (~/.bashrc, ~/.zshrc, etc.)
echo "export SCRAPER_PATH=\"$INSTALL_DIR\"" >> ~/.zshrc
echo "alias scraper-status='python3 \$SCRAPER_PATH/skill/scripts/scraper_status.py'" >> ~/.zshrc
echo "alias scraper='python3 \$SCRAPER_PATH/skill/scripts/scraper_cli.py'" >> ~/.zshrc
source ~/.zshrc

# === PASO 3: Instalar skill para Claude Code ===
mkdir -p ~/.agents/skills
ln -s "$INSTALL_DIR/skill" ~/.agents/skills/gmaps-scraper

# === PASO 4: Verificar instalacion ===
scraper-status
```

### Permisos y Directorios

El scraper necesita permisos de escritura en varios directorios:

| Directorio | Proposito | Permisos |
|------------|-----------|----------|
| `$SCRAPER_PATH/scrappings/` | CSVs de resultados | Escritura |
| `$SCRAPER_PATH/cache/` | Checkpoints, jobs, control | Escritura |
| `$SCRAPER_PATH/browser_data/` | Cookies de Google Maps | Escritura |
| `$SCRAPER_PATH/*.log` | Logs de ejecucion | Escritura |

Si instalas en `/opt` o ubicacion compartida, asegurate de que el usuario tenga permisos:

```bash
# Para instalacion en /opt (uso compartido)
sudo mkdir -p /opt/scrapping
sudo chown -R $USER:$USER /opt/scrapping
git clone https://github.com/gorkavidal/scrapping.git /opt/scrapping
```

### Gestion de Archivos de Salida

Los resultados se acumulan en `scrappings/`. Para gestionar el espacio:

```bash
# Ver espacio usado
du -sh $SCRAPER_PATH/scrappings/

# Listar archivos por tamaño
ls -lhS $SCRAPER_PATH/scrappings/

# Archivar resultados antiguos (mas de 30 dias)
mkdir -p $SCRAPER_PATH/scrappings/archive
find $SCRAPER_PATH/scrappings -name "*.csv" -mtime +30 -exec mv {} $SCRAPER_PATH/scrappings/archive/ \;

# Comprimir archivo
tar -czvf $SCRAPER_PATH/scrappings/archive_$(date +%Y%m).tar.gz $SCRAPER_PATH/scrappings/archive/
rm -rf $SCRAPER_PATH/scrappings/archive/
```

### Uso del Skill

Una vez instalado, el AI assistant puede usar estos comandos:

#### Status rapido (uso frecuente)
```bash
python3 $SCRAPER_PATH/skill/scripts/scraper_status.py
```

Salida ejemplo:
```
============================================================
GOOGLE MAPS SCRAPER STATUS
============================================================

Cookies: ✓ Cookies valid (3 days old)

--- Active Jobs (1) ---

  [RUNNING] dentistas in ES
    PID: 12345 | Workers: 2
    Progress: 45/120 cities (37.5%)
    Results: 1523 with email (892 corporate)
    Runtime: 2h 15m | ETA: 3h 45m
    Current: Valencia

--- Recent History (5 total) ---
  ● restaurantes (ES) - 3450 emails - completed
  ○ hoteles (FR) - 890 emails - interrupted [resumable]
```

#### Iniciar nuevo scraping
```bash
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py start \
  --query "clinicas dentales" \
  --country ES \
  --min-pop 50000 \
  --workers 1
```

#### Controlar jobs
```bash
# Pausar (libera recursos, guarda checkpoint)
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py pause <job_id>

# Reanudar
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py resume <job_id>

# Parar gracefully
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py stop <job_id>
```

#### Acceder a resultados
```bash
# Listar archivos de resultados
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py results --list

# Resumen detallado de un job (archivos, stats)
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py results <job_id> --summary

# Mostrar ultimos N registros
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py results <job_id> --show 20

# Exportar en formato JSON
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py results <job_id> --show 50 --format json

# Exportar en formato TSV (para hojas de calculo)
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py results <job_id> --show 100 --format tsv

# Obtener solo la ruta del CSV (para scripts)
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py results <job_id> --path

# Ver ubicacion de todos los archivos
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py files
```

### Principios de Uso de Recursos

El skill esta diseñado para ser **conservador con los recursos**:

| RAM del Sistema | Workers Recomendados | Jobs Simultaneos |
|-----------------|---------------------|------------------|
| 8GB             | 1                   | 1                |
| 16GB            | 1-2                 | 1                |
| 32GB+           | 2-3                 | 1-2              |

**Reglas para el AI assistant**:
- Por defecto, usar 1 worker
- Nunca sugerir mas de 3 workers sin que el usuario lo pida explicitamente
- Recomendar pausar si el sistema esta bajo carga
- Los resultados se guardan cada 10 negocios, es seguro interrumpir

### Actualizacion del Skill

```bash
cd $SCRAPER_PATH
git pull origin main
```

Si usaste enlace simbolico, el skill se actualiza automaticamente. Si copiaste los archivos manualmente, repite el proceso de copia.

### Directorio de Salida Personalizado

Por defecto, los resultados se guardan en `$SCRAPER_PATH/scrappings/`. Puedes configurar un directorio diferente:

```bash
# Opcion 1: Variable de entorno (global)
export SCRAPER_OUTPUT_DIR="/home/user/leads/gmaps"

# Opcion 2: Argumento por job
python3 $SCRAPER_PATH/skill/scripts/scraper_cli.py start \
  --query "dentistas" --country ES --output-dir /path/to/results
```

**Prioridad de directorios:**
1. `--output-dir` (argumento por job)
2. `SCRAPER_OUTPUT_DIR` (variable de entorno)
3. `$SCRAPER_PATH/scrappings/` (default)

### Archivos Generados

Cada job de scraping genera dos archivos CSV:

| Archivo | Contenido |
|---------|-----------|
| `results_<PAIS>_<QUERY>_<ID>_<FECHA>.csv` | Negocios CON email encontrado |
| `no_emails_<PAIS>_<QUERY>_<ID>_<FECHA>.csv` | Negocios SIN email (tienen web) |

**Columnas del CSV:**

| Columna | Descripcion |
|---------|-------------|
| `Name` | Nombre del negocio |
| `Localidad` | Ciudad |
| `Region` | Comunidad autonoma/estado |
| `Address` | Direccion completa |
| `Phone` | Telefono |
| `Rating` | Valoracion en Google Maps |
| `Website` | URL de la web |
| `Email_Raw` | Todos los emails encontrados |
| `Email_Filtered` | Solo emails corporativos (coinciden con dominio web) |
| `Email_Search_Status` | Estado de la busqueda de email |
| `ID` | Identificador unico |

### Troubleshooting del Skill

| Problema | Solucion |
|----------|----------|
| "SCRAPER_PATH not set" | Añadir `export SCRAPER_PATH=...` a tu shell config |
| "No module named 'job_manager'" | Verificar que SCRAPER_PATH apunta al directorio correcto |
| "Permission denied" | Verificar permisos de escritura en los directorios |
| "Cookies expired" | Ejecutar `python scrape_maps_interactive.py --setup` |
| "Output directory does not exist" | Crear el directorio antes de usarlo |
| Skill no aparece en Claude Code | Verificar que existe `~/.agents/skills/gmaps-scraper/SKILL.md` |
