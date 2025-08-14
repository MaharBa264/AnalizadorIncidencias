# /analizador/main/routes.py
# Rutas principales de la aplicación: panel de control, filtros, descargas, etc.

import os
import io
import pandas as pd
import threading
import pytz
import math
from collections import defaultdict, Counter
from datetime import datetime, date, time, timedelta
from flask import render_template, request, redirect, url_for, jsonify, current_app, send_file, flash
from flask_login import login_required, current_user
from . import main
from .. import influx_client, INFLUXDB_ORG, INFLUXDB_BUCKET
from ..services import process_file_to_influxdb
from ..decorators import admin_required
#from .. import (
#    weather_influx_client,
#    WEATHER_INFLUX_ORG,
#)
from .. import weather_influx_client
from ..weather_adapter import load_distrito_tags, cross_incidents_with_weather

# =========================
# Fechas y zona horaria
# =========================
TZ = pytz.timezone("America/Argentina/San_Luis")

def _to_local(dt: datetime) -> datetime:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return TZ.localize(dt)
    return dt.astimezone(TZ)


def _coerce_float(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace(',', '.')
        return float(s)
    except Exception:
        return default


def _coerce_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, (int,)):
            return int(x)
        s = str(x).strip()
        return int(float(s.replace(',', '.')))
    except Exception:
        return default

def _parse_date_or_none(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def _safe_date(s):
    """Recibe 'YYYY-MM-DD' y devuelve datetime o None."""
    return _parse_date_or_none(s)

def _build_available_end_dates(start_dt):
    """
    Usa tu función existente get_available_dates() (strings 'YYYY-MM-DD')
    y filtra >= start_dt.
    """
    if not start_dt:
        return []
    start_s = start_dt.strftime("%Y-%m-%d")
    all_dates = get_available_dates()  # ya la usas para el combo 'Desde'
    return [d for d in all_dates if d >= start_s]


def _incident_iter(incidents):
    """Itera incidentes normalizados con dt_inicio_local, dt_fin_local y dur_min."""
    for inc in incidents:
        f_ini = inc.get('fecha_inicio_fmt') or inc.get('fecha_inicio')
        h_ini = inc.get('hora_inicio_fmt')  or inc.get('hora_inicio')
        f_fin = inc.get('fecha_fin_fmt')    or inc.get('fecha_fin')
        h_fin = inc.get('hora_fin_fmt')     or inc.get('hora_fin')

        dt_ini = _parse_datetime_flexible(f_ini, h_ini)
        dt_fin = _parse_datetime_flexible(f_fin, h_fin)
        if not dt_ini or not dt_fin:
            continue
        dt_ini = _to_local(dt_ini)
        dt_fin = _to_local(dt_fin)
        if dt_fin < dt_ini:
            continue
        dur_min = max(0, (dt_fin - dt_ini).total_seconds() / 60.0)
        nivel = inc.get('nivel_tension') or inc.get('nivel_tensión') or inc.get('nivel') or 'N/D'
        causa = inc.get('descripcion_de_la_causa') or 'Sin causa'
        nises = _coerce_int(inc.get('nises_involucrados'))
        pot   = _coerce_float(inc.get('potencia_involucrada'))

        yield {
            'distrito': str(inc.get('distrito') or inc.get('Distrito') or inc.get('DISTRITO') or '').strip(),
            'dt_inicio_local': dt_ini,
            'dt_fin_local': dt_fin,
            'dur_min': dur_min,
            'nivel_tension': nivel,
            'causa': causa,
            'nises': nises,
            'potencia': pot,
        }

def _fetch_incidents_with_filters():
    sd = _parse_date_flexible(request.args.get('start_date'))
    ed = _parse_date_flexible(request.args.get('end_date'))
    start_date = sd.date() if sd else None
    end_date   = ed.date() if ed else None

    distrito = (request.args.get('distrito') or '').strip() or None
    causa    = (request.args.get('causa') or '').strip() or None
    nivel    = (request.args.get('nivel_tension') or '').strip() or None

    incidents = get_filtered_incidents(start_date, end_date, distrito, causa, nivel)
    return list(_incident_iter(incidents))


def _to_date_any(x):
    if isinstance(x, datetime): return x.date()
    if isinstance(x, date):     return x
    s = str(x).strip() if x is not None else ""
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None

def _day_range_local_to_utc(d):
    start_local = TZ.localize(datetime.combine(d, time.min))
    end_local   = TZ.localize(datetime.combine(d, time.max))
    return start_local.astimezone(pytz.utc), end_local.astimezone(pytz.utc)

def _as_date(x):
    """Devuelve un date a partir de date|datetime|None, sin reventar."""
    if x is None:
        return None
    return x.date() if isinstance(x, datetime) else x



def _parse_date_flexible(s):
    """Acepta DD-MM-YYYY o YYYY-MM-DD y devuelve datetime (sin TZ)."""
    if not s:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None  # si no matchea ninguno


def _parse_datetime_flexible(date_str, time_str):
    """Combina una fecha (flexible) y una hora (HH:MM[:SS]) en datetime.
    Si no puede parsear, devuelve 1900-01-01 00:00:00 para no romper el sort.
    """
    d = _parse_date_flexible(date_str)
    if d is None:
        return datetime(1900, 1, 1)
    h = (time_str or "00:00:00").strip()
    t = None
    for tfmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(h, tfmt).time()
            break
        except Exception:
            pass
    if t is None:
        t = datetime.min.time()
    return datetime.combine(d, t)



def get_filter_options():
    """Obtiene distritos y causas desde tags indexados de InfluxDB."""
    distritos, causas = [], []
    try:
        query_api = influx_client.query_api()

        q_distritos = f"""
            import \"influxdata/influxdb/schema\"
            schema.tagValues(bucket: \"{INFLUXDB_BUCKET}\", tag: \"distrito\", start: -5y)
        """
        q_causas = f"""
            import \"influxdata/influxdb/schema\"
            schema.tagValues(bucket: \"{INFLUXDB_BUCKET}\", tag: \"descripcion_de_la_causa\", start: -5y)
        """

        result_distritos = query_api.query(q_distritos, org=INFLUXDB_ORG)
        result_causas = query_api.query(q_causas, org=INFLUXDB_ORG)

        distritos = [row.values["_value"] for table in result_distritos for row in table.records]
        causas = [row.values["_value"] for table in result_causas for row in table.records]

    except Exception as e:
        print(f"Error obteniendo opciones de filtro: {e}")

    return sorted(distritos), sorted(causas)


def get_available_dates():
    """Obtiene fechas únicas ordenadas con datos en el bucket."""
    dates = set()
    try:
        query_api = influx_client.query_api()
        query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -5y)
                |> filter(fn: (r) => r._measurement == "incidencia_electrica")
                |> keep(columns: ["_time"])
        '''
        result = query_api.query(query, org=INFLUXDB_ORG)
        for table in result:
            for record in table.records:
                date_str = record.get_time().strftime('%Y-%m-%d')
                dates.add(date_str)
    except Exception as e:
        print(f"Error obteniendo fechas: {e}")
    return sorted(dates)


def get_filtered_incidents(start_date, end_date, distrito, causa, nivel_tension=None):
    """Construye y ejecuta una query de Flux dinámica basada en los filtros."""
    processed_incidents = []
    try:
        query_api = influx_client.query_api()
        query_parts = [f'from(bucket: "{INFLUXDB_BUCKET}")']

        # ---- Construcción de rango (local -03:00 → UTC) ----
        if start_date:
            effective_end = end_date or start_date  # si no hay "Hasta", usamos el mismo día
            # 00:00 local del día inicio  → UTC
            start_local = TZ.localize(datetime.combine(start_date, time(0, 0, 0)))
            # 00:00 local del día siguiente a "effective_end" (stop exclusivo) → UTC
            stop_local  = TZ.localize(datetime.combine(effective_end, time(0, 0, 0))) + timedelta(days=1)

            start_utc = start_local.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            stop_utc  = stop_local.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

            query_parts.append(f'|> range(start: {start_utc}, stop: {stop_utc})')
        else:
            # Rango amplio por defecto (evita unbounded read)
            query_parts.append('|> range(start: -5y)')

        # Medición y pivot (igual que ahora)
        query_parts += [
            '|> filter(fn: (r) => r._measurement == "incidencia_electrica")',
            '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        ]

        # Filtros existentes
        if distrito:
            query_parts.append(f'|> filter(fn: (r) => r.distrito == "{distrito}")')
        if causa:
            causa_escaped = causa.replace('\\', '\\\\').replace('"', '\\"')
            query_parts.append(f'|> filter(fn: (r) => r.descripcion_de_la_causa == "{causa_escaped}")')

        # 🆕 Filtro de nivel de tensión
        if nivel_tension in ("BT", "MT"):
            query_parts.append(f'|> filter(fn: (r) => r.nivel_tension == "{nivel_tension}")')

        query_parts.append('|> sort(columns: ["_time"], desc: true)')
        query = "\n".join(query_parts)

        tables = query_api.query(query, org=INFLUXDB_ORG)
        incidents_data = [record.values for table in tables for record in table.records]
        for incident in incidents_data:
            # Normalización para mostrar (tu lógica actual)
            incident['fecha_inicio_fmt'] = incident.get('fecha_inicio', '')
            incident['hora_inicio_fmt']  = incident.get('hora_inicio', '')
            incident['fecha_fin_fmt']    = incident.get('fecha_fin', '')
            incident['hora_fin_fmt']     = incident.get('hora_fin', '')
            processed_incidents.append(incident)
    except Exception as e:
        print(f"Error al ejecutar la query de filtro: {e}")
    return processed_incidents

def compute_total_duration(incidents):
    """Suma el tiempo (fin - inicio) de cada incidente.
    Devuelve (string_legible, total_minutos)."""
    total = timedelta(0)
    for inc in incidents:
        # Usamos *_fmt si existen; si no, tomamos los originales
        f_ini = inc.get('fecha_inicio_fmt') or inc.get('fecha_inicio')
        h_ini = inc.get('hora_inicio_fmt')  or inc.get('hora_inicio')
        f_fin = inc.get('fecha_fin_fmt')    or inc.get('fecha_fin')
        h_fin = inc.get('hora_fin_fmt')     or inc.get('hora_fin')

        sd = _parse_datetime_flexible(f_ini, h_ini)
        ed = _parse_datetime_flexible(f_fin, h_fin)
        if ed >= sd:
            total += (ed - sd)

    total_seconds = int(total.total_seconds())
    minutes = (total_seconds // 60) % 60
    hours   = (total_seconds // 3600) % 24
    days    =  total_seconds // (24 * 3600)

    parts = []
    if days:
        parts.append(f"{days} días")
    if hours:
        parts.append(f"{hours} h")
    # Mostrar minutos aunque todo sea 0
    parts.append(f"{minutes} min")

    return " ".join(parts), (total_seconds // 60)

@main.route("/api/end_dates")
@login_required
def api_end_dates():
    frm = (request.args.get("from") or "").strip()
    d_from_dt = _parse_date_flexible(frm)
    if d_from_dt is None:
        return jsonify({"dates": []})
    d_from = d_from_dt.date()

    raw_all = get_available_dates()  # strings en distintos formatos
    all_dt = sorted({
        (_parse_date_flexible(d).date())
        for d in raw_all
        if _parse_date_flexible(d) is not None
    })
    end_dates = [d.strftime("%d-%m-%Y") for d in all_dt if d >= d_from]
    return jsonify({"dates": end_dates})


@main.route('/', methods=['GET', 'POST'])
@login_required
def index():
    distritos, causas = get_filter_options()
    incidents = []
    form_data = {}

    # Fechas disponibles (siempre cargar "Desde")
    raw_all_dates = get_available_dates()
    all_dt = sorted({(_parse_date_flexible(d).date()) for d in raw_all_dates if _parse_date_flexible(d) is not None})
    available_start_dates = [dt.strftime("%d-%m-%Y") for dt in all_dt]

    if request.method == 'GET':
        return render_template(
            'index.html',
            name=current_user.name,
            distritos=distritos,
            causas=causas,
            form_data=form_data,
            incidents=incidents,
            available_start_dates=available_start_dates,
            available_end_dates=[],
            total_duration_str=None,
            total_duration_minutes=None,
        )

    # POST
    sd = _parse_date_flexible(request.form.get('start_date'))
    ed = _parse_date_flexible(request.form.get('end_date'))
    start_date = sd.date() if sd else None
    end_date   = ed.date() if ed else None

    distrito       = (request.form.get('distrito') or '').strip() or None
    causa          = (request.form.get('causa') or '').strip() or None
    nivel_tension  = (request.form.get('nivel_tension') or '').strip() or None  # 🆕

    traer_tabla = bool(request.form.get('traer_tabla'))  # 🆕
    graficar    = bool(request.form.get('graficar'))     # 🆕

    # Guardar estado del formulario
    form_data.update({
        'start_date': request.form.get('start_date') or '',
        'end_date':   request.form.get('end_date') or '',
        'distrito':   distrito or '',
        'causa':      causa or '',
        'nivel_tension': nivel_tension or '',
        'traer_tabla': traer_tabla,
        'graficar': graficar,
    })

    # Validaciones de fechas (misma lógica que tenías)
    if end_date and not start_date:
        flash("Seleccione primero la fecha 'Desde'.", "warning")
        return render_template('index.html', name=current_user.name, distritos=distritos, causas=causas,
                               form_data=form_data, incidents=[],
                               available_start_dates=available_start_dates,
                               available_end_dates=[],
                               total_duration_str=None, total_duration_minutes=None)

    if start_date and end_date and end_date < start_date:
        flash("La fecha 'Hasta' no puede ser anterior a 'Desde'.", "warning")
        available_end_dates = [d.strftime("%d-%m-%Y") for d in all_dt if d >= start_date]
        return render_template('index.html', name=current_user.name, distritos=distritos, causas=causas,
                               form_data=form_data, incidents=[],
                               available_start_dates=available_start_dates,
                               available_end_dates=available_end_dates,
                               total_duration_str=None, total_duration_minutes=None)

    # Armar "Hasta" a partir de "Desde"
    available_end_dates = [d.strftime("%d-%m-%Y") for d in all_dt if (not start_date or d >= start_date)]

    # Exclusividad de checkboxes
    if traer_tabla and graficar:
        flash("No se pueden seleccionar 'Traer tabla' y 'Graficar' al mismo tiempo.", "warning")
        return render_template('index.html', name=current_user.name, distritos=distritos, causas=causas,
                               form_data=form_data, incidents=[],
                               available_start_dates=available_start_dates,
                               available_end_dates=available_end_dates,
                               total_duration_str=None, total_duration_minutes=None)

    # Si el usuario eligió 'Graficar' -> ir a la página de gráficos con los filtros como querystring
    if graficar:
        params = {
            'start_date': form_data['start_date'],
            'end_date':   form_data['end_date'],
            'distrito':   form_data['distrito'],
            'causa':      form_data['causa'],
            'nivel_tension': form_data['nivel_tension'],
        }
        return redirect(url_for('main.graficos', **params))

    # Caso "Traer tabla" (o ninguno marcado: por defecto tabla)
    if (not start_date and not end_date) and (distrito or causa or nivel_tension):
        incidents = get_filtered_incidents(None, None, distrito, causa, nivel_tension)
    elif start_date:
        effective_end = end_date or start_date
        incidents = get_filtered_incidents(start_date, effective_end, distrito, causa, nivel_tension)
    else:
        incidents = []

    # Orden cronológico robusto
    incidents.sort(key=lambda inc: _parse_datetime_flexible(
        inc.get('fecha_inicio_fmt', '01-01-1900'), inc.get('hora_inicio_fmt', '00:00:00')
    ))

    # Resumen de tiempos acumulados
    total_duration_str, total_duration_minutes = (None, None)
    if incidents:
        total_duration_str, total_duration_minutes = compute_total_duration(incidents)

    return render_template(
        'index.html',
        name=current_user.name,
        distritos=distritos,
        causas=causas,
        form_data=form_data,
        incidents=incidents,
        available_start_dates=available_start_dates,
        available_end_dates=available_end_dates,
        total_duration_str=total_duration_str,
        total_duration_minutes=total_duration_minutes,
    )

@main.route('/upload_page')
@login_required
def upload_page():
    """Muestra la página dedicada a la carga de archivos."""
    return render_template('upload.html', name=current_user.name)

@main.route('/upload', methods=['POST'])
@login_required
def upload_file():
    if 'file' not in request.files:
        flash('No se seleccionó ningún archivo.')
        return redirect(url_for('main.upload_page'))
    file = request.files['file']
    if file.filename == '':
        flash('No se seleccionó ningún archivo.')
        return redirect(url_for('main.upload_page'))
    allowed_extensions = ('.csv', '.xls', '.xlsx')
    if file and file.filename.lower().endswith(allowed_extensions):
        original_filename = file.filename
        name, extension = os.path.splitext(original_filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_filename = f"{name}_{timestamp}{extension}"
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], new_filename)
        file.save(filepath)
        process_file_to_influxdb(filepath)
        flash(f"Archivo '{original_filename}' procesado exitosamente.", "success")
        return redirect(url_for('main.upload_page'))
    flash('Formato de archivo no válido. Por favor, sube un archivo CSV o Excel.', 'error')
    return redirect(url_for('main.upload_page'))

@main.route('/download_xls')
@login_required
def download_xls():
    # _parse_date_flexible devuelve datetime; lo pasamos a date para ser coherentes
    start_dt = _parse_date_flexible(request.args.get('start_date'))
    end_dt   = _parse_date_flexible(request.args.get('end_date'))
    start_date = start_dt.date() if start_dt else None
    end_date   = end_dt.date()   if end_dt   else None
    distrito = request.args.get('distrito')
    causa = request.args.get('causa')
    nivel_tension = request.args.get('nivel_tension')

    incidents = get_filtered_incidents(start_date, end_date, distrito, causa, nivel_tension)

    if not incidents:
        return "No hay datos para descargar con los filtros seleccionados.", 404

    df = pd.DataFrame(incidents)

    df_export = pd.DataFrame({
        'Nro. Incidencia': df.get('nro_incidencia', ''),
        'Fecha de Inicio': df.get('fecha_inicio_fmt', ''),
        'Hora de Inicio': df.get('hora_inicio_fmt', ''),
        'Fecha de Fin': df.get('fecha_fin_fmt', ''),
        'Hora de Fin': df.get('hora_fin_fmt', ''),
        'Distrito': df.get('distrito', ''),
        'Nivel de Tensión': df.get('nivel_tension', ''),
        'Instalación': df.get('instalacion', ''),
        'Localidad': df.get('localidad', ''),
        'Distribuidor': df.get('distribuidor', ''),
        'Causa': df.get('descripcion_de_la_causa', ''),
        'Reclamos': df.get('cantidad_de_reclamos', ''),
        'CT Involucrados': df.get('ct_involucrados', ''),
        'Clientes Afectados': df.get('nises_involucrados', ''),
        'Potencia Involucrada': df.get('potencia_involucrada', '')
    })

    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df_export.to_excel(writer, index=False, sheet_name='Incidencias')
    writer.close()
    output.seek(0)

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='reporte_incidencias.xlsx'
    )

@main.route('/filtros_opciones', methods=['GET'])
@login_required
def filtros_opciones():
    try:
        distritos, causas = get_filter_options()  # usa tu helper actual
    except Exception:
        distritos, causas = [], []
    # Nivel de tensión lo acotamos a las dos opciones pedidas
    return jsonify({
        'distritos': distritos,
        'causas': causas,
        'nivel_tension': ['BT', 'MT']
    })

@main.route('/graficos', methods=['GET'])
@login_required
def graficos():
    # Leemos filtros por querystring
    form_data = {
        'start_date': (request.args.get('start_date') or '').strip(),
        'end_date':   (request.args.get('end_date') or '').strip(),
        'distrito':   (request.args.get('distrito') or '').strip(),
        'causa':      (request.args.get('causa') or '').strip(),
        'nivel_tension': (request.args.get('nivel_tension') or '').strip(),
    }
    return render_template('graficos.html', name=current_user.name, form_data=form_data)

@main.route('/api/graficos/kpis', methods=['GET'])
@login_required
def api_kpis():
    rows = _fetch_incidents_with_filters()
    total_inc = len(rows)
    total_min = sum(r['dur_min'] for r in rows)
    clientes_min = sum(r['nises'] * r['dur_min'] for r in rows)
    potencia_total = sum(r['potencia'] for r in rows)

    return jsonify({
        'incidencias': total_inc,
        'total_minutos': round(total_min, 2),
        'clientes_min': round(clientes_min, 2),
        'potencia_total_kw': round(potencia_total, 2)
    })

@main.route('/api/graficos/serie_incidencias', methods=['GET'])
@login_required
def api_serie_incidencias():
    rows = _fetch_incidents_with_filters()
    # Conjunto de días presentes
    dias = set()
    for r in rows:
        dias.add(r['dt_inicio_local'].date())
    if not dias:
        return jsonify({'dates': [], 'total': [], 'BT': [], 'MT': []})

    dates_sorted = sorted(dias)
    idx = {d: i for i, d in enumerate(dates_sorted)}
    total = [0]*len(dates_sorted)
    bt    = [0]*len(dates_sorted)
    mt    = [0]*len(dates_sorted)

    for r in rows:
        i = idx[r['dt_inicio_local'].date()]
        total[i] += 1
        nv = (r['nivel_tension'] or '').upper()
        if nv == 'BT':
            bt[i] += 1
        elif nv == 'MT':
            mt[i] += 1

    return jsonify({
        'dates': [d.strftime('%Y-%m-%d') for d in dates_sorted],
        'total': total,
        'BT': bt,
        'MT': mt,
    })


@main.route('/api/graficos/serie_duracion', methods=['GET'])
@login_required
def api_serie_duracion():
    rows = _fetch_incidents_with_filters()
    dias = set(r['dt_inicio_local'].date() for r in rows)
    if not dias:
        return jsonify({'dates': [], 'total': [], 'BT': [], 'MT': []})

    dates_sorted = sorted(dias)
    idx = {d: i for i, d in enumerate(dates_sorted)}
    total = [0.0]*len(dates_sorted)
    bt    = [0.0]*len(dates_sorted)
    mt    = [0.0]*len(dates_sorted)

    for r in rows:
        i = idx[r['dt_inicio_local'].date()]
        total[i] += r['dur_min']
        nv = (r['nivel_tension'] or '').upper()
        if nv == 'BT':
            bt[i] += r['dur_min']
        elif nv == 'MT':
            mt[i] += r['dur_min']

    return jsonify({
        'dates': [d.strftime('%Y-%m-%d') for d in dates_sorted],
        'total': [round(x,2) for x in total],
        'BT': [round(x,2) for x in bt],
        'MT': [round(x,2) for x in mt],
    })

@main.route('/api/graficos/pareto_causas', methods=['GET'])
@login_required
def api_pareto_causas():
    metric = (request.args.get('metric') or 'incidencias').lower()
    rows = _fetch_incidents_with_filters()

    agg = defaultdict(float)
    for r in rows:
        key = r['causa']
        if metric == 'minutos':
            agg[key] += r['dur_min']
        else:
            agg[key] += 1.0

    # Orden descendente y top N (p.ej., top 12) + "Otros"
    items = sorted(agg.items(), key=lambda kv: kv[1], reverse=True)
    TOPN = 12
    top = items[:TOPN]
    rest = items[TOPN:]
    if rest:
        otros_val = sum(v for _, v in rest)
        top.append(('Otros', otros_val))

    categories = [k for k,_ in top]
    values = [round(v,2) for _,v in top]

    # Cálculo de acumulada (%)
    total = sum(values) or 1.0
    acumulada = []
    run = 0.0
    for v in values:
        run += v
        acumulada.append(round(100.0*run/total, 2))

    return jsonify({
        'categories': categories,
        'values': values,
        'acumulada_pct': acumulada
    })

@main.route('/api/graficos/heatmap_horadia', methods=['GET'])
@login_required
def api_heatmap_horadia():
    """Mapa de calor hora (0–23) × día de semana (Lun–Dom).
    Parámetro opcional: metric=incidencias|minutos (default: minutos)
    """
    metric = (request.args.get('metric') or 'minutos').lower()
    rows = _fetch_incidents_with_filters()

    # Ejes
    horas = list(range(24))
    week_labels = ['Lun','Mar','Mié','Jue','Vie','Sáb','Dom']  # Monday=0

    # Matriz 7x24 inicializada en 0
    mat = [[0.0 for _ in horas] for _ in range(7)]

    for r in rows:
        dow = r['dt_inicio_local'].weekday()  # 0=Lun ... 6=Dom
        h   = r['dt_inicio_local'].hour
        if metric == 'incidencias':
            mat[dow][h] += 1.0
        else:
            mat[dow][h] += r['dur_min']

    # Convertimos a tripletas [x(hour), y(dow), value]
    data = []
    vmax = 0.0
    for y in range(7):
        for x in horas:
            v = round(mat[y][x], 2)
            vmax = max(vmax, v)
            data.append([x, y, v])

    return jsonify({
        'hours': horas,
        'weekdays': week_labels,
        'data': data,
        'max': round(vmax, 2),
        'metric': metric
    })

@main.route('/api/graficos/histo_duracion', methods=['GET'])
@login_required
def api_histo_duracion():
    """Histograma por bins de duración en minutos.
    Bins: <15, 15–60, 60–120, 120–240, >240.
    Devuelve series Total, BT y MT (barras apiladas).
    """
    rows = _fetch_incidents_with_filters()

    bins = [
        ('<15',    lambda m: m < 15),
        ('15–60',  lambda m: 15 <= m < 60),
        ('1–2h',   lambda m: 60 <= m < 120),
        ('2–4h',   lambda m: 120 <= m < 240),
        ('>4h',    lambda m: m >= 240),
    ]

    cats = [b[0] for b in bins]
    tot = [0]*len(bins)
    bt  = [0]*len(bins)
    mt  = [0]*len(bins)

    def bin_index(m):
        for i, (_, cond) in enumerate(bins):
            if cond(m):
                return i
        return len(bins)-1

    for r in rows:
        i = bin_index(r['dur_min'])
        tot[i] += 1
        nv = (r['nivel_tension'] or '').upper()
        if nv == 'BT':
            bt[i] += 1
        elif nv == 'MT':
            mt[i] += 1

    return jsonify({
        'categories': cats,
        'total': tot,
        'BT': bt,
        'MT': mt
    })

@main.route('/comparar_clima', methods=['GET', 'POST'])
@login_required
def comparar_clima():
    # Opciones para selects
    distritos, _ = get_filter_options()
    available_start_dates = get_available_dates()

    form_data = {
        'start_date': '',
        'end_date': '',
        'distrito': '',
        'nivel_tension': ''
    }

    df_result = None

    # --- GET: permitir precargar "Hasta" cuando cambia "Desde" (via querystring) ---
    if request.method == 'GET':
        qs_start = (request.args.get('start_date') or '').strip()
        if qs_start:
            form_data['start_date'] = qs_start
            sd = _parse_date_or_none(qs_start)
            available_end_dates = _build_available_end_dates(sd) if sd else []
        else:
            available_end_dates = []
        return render_template(
            'comparar_clima.html',
            name=current_user.name,
            distritos=distritos,
            available_start_dates=available_start_dates,
            available_end_dates=available_end_dates,
            form_data=form_data,
            df_result=None
        )

    # --- POST ---
    sd = _parse_date_or_none(request.form.get('start_date'))
    ed = _parse_date_or_none(request.form.get('end_date'))
    start_date = sd.date() if sd else None
    end_date   = ed.date() if ed else None

    distrito      = (request.form.get('distrito') or '').strip() or None
    nivel_tension = (request.form.get('nivel_tension') or '').strip() or None

    form_data.update({
        'start_date': request.form.get('start_date') or '',
        'end_date':   request.form.get('end_date') or '',
        'distrito':   distrito or '',
        'nivel_tension': nivel_tension or '',
    })

    # Validaciones de fechas
    if end_date and not start_date:
        flash("Seleccione primero la fecha 'Desde'.", "warning")
        return render_template(
            'comparar_clima.html',
            name=current_user.name,
            distritos=distritos,
            available_start_dates=available_start_dates,
            available_end_dates=[],
            form_data=form_data,
            df_result=None
        )

    if start_date and end_date and end_date < start_date:
        flash("'Hasta' no puede ser anterior a 'Desde'.", "warning")
        return render_template(
            'comparar_clima.html',
            name=current_user.name,
            distritos=distritos,
            available_start_dates=available_start_dates,
            available_end_dates=_build_available_end_dates(sd) if sd else [],
            form_data=form_data,
            df_result=None
        )

    # Traer incidencias con tus helpers
    if start_date:
        effective_end = end_date or start_date
        base_incidents = get_filtered_incidents(start_date, effective_end, distrito, None, nivel_tension)
    else:
        base_incidents = get_filtered_incidents(None, None, distrito, None, nivel_tension)

    # Normalizar a estructura con dt_inicio_local / dt_fin_local / dur_min
    norm = list(_incident_iter(base_incidents))

    # Si no hay incidencias, devolvemos info y no seguimos al clima
    if not norm:
        flash("No hay incidencias para los filtros elegidos (rango y/o distrito).", "info")
        return render_template(
            'comparar_clima.html',
            name=current_user.name,
            distritos=distritos,
            available_start_dates=available_start_dates,
            available_end_dates=_build_available_end_dates(sd) if sd else [],
            form_data=form_data,
            df_result=None
        )

    # CSV de mapeo distrito → tag textual
    csv_path = os.path.join(current_app.instance_path, 'distritos_weather_tag.csv')
    try:
        dmap = load_distrito_tags(csv_path)
    except Exception as e:
        flash(f"No se pudo leer CSV de distritos/tags: {e}", 'danger')
        dmap = {}

    # Clima (cliente remoto)
    w_query_api = weather_influx_client.query_api()
    try:
        df_result = cross_incidents_with_weather(w_query_api, norm, dmap)
    except Exception as e:
        flash(f"Error consultando clima: {e}", 'danger')
        df_result = None

    if df_result is None or df_result.empty:
        flash("No hay resultados para los filtros elegidos (¿hay incidencias en ese rango/distrito?).", "info")
    else:
        # Diagnóstico rápido en logs
        current_app.logger.info(f"[comparar_clima] columnas resultado: {list(df_result.columns)}")
        # Orden “seguro”: solo por columnas presentes
        sort_keys = [c for c in ['distrito', 'dt_inicio_local', 'dt_fin_local'] if c in df_result.columns]
        if sort_keys:
            df_result = df_result.sort_values(by=sort_keys)
        else:
            flash("Resultados sin columnas de orden estándar (distrito/fechas). Revisar normalización de incidencias.", "warning")


    if df_result is not None and not df_result.empty:
        df_result = df_result.sort_values(by=['distrito', 'dt_inicio_local'])

    # "Hasta" para este POST (en base a "Desde")
    available_end_dates = _build_available_end_dates(sd) if sd else []

    return render_template(
        'comparar_clima.html',
        name=current_user.name,
        distritos=distritos,
        available_start_dates=available_start_dates,
        available_end_dates=available_end_dates,
        form_data=form_data,
        df_result=df_result
    )

@main.route('/admin')
@login_required
@admin_required
def admin_page():
    return render_template('admin.html', name=current_user.name)

@main.route('/purge', methods=['POST'])
@login_required
@admin_required
def purge_data():
    def run_purge():
        try:
            buckets_api = influx_client.buckets_api()
            bucket = buckets_api.find_bucket_by_name(INFLUXDB_BUCKET)
            if bucket:
                buckets_api.delete_bucket(bucket)
                buckets_api.create_bucket(bucket_name=INFLUXDB_BUCKET, org=INFLUXDB_ORG)
                print("✔ Bucket eliminado y recreado exitosamente.")
            else:
                print(f"⚠ Bucket '{INFLUXDB_BUCKET}' no encontrado.")
        except Exception as e:
            print(f"❌ Error durante recreación de bucket: {e}")

    threading.Thread(target=run_purge).start()
    flash("El bucket fue purgado y recreado. Puede demorar unos segundos en verse reflejado.", "info")
    return redirect(url_for('main.admin_page'))

