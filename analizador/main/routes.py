# /analizador/main/routes.py
# Rutas principales de la aplicación: panel de control, filtros, descargas, etc.

import os
import io
import pandas as pd
import threading
import pytz
from datetime import datetime, date, time, timedelta
from flask import render_template, request, redirect, url_for, jsonify, current_app, send_file, flash
from flask_login import login_required, current_user
from . import main
from .. import influx_client, INFLUXDB_ORG, INFLUXDB_BUCKET
from ..services import process_file_to_influxdb
from ..decorators import admin_required

# =========================
# Fechas y zona horaria
# =========================
TZ = pytz.timezone("America/Argentina/San_Luis")

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


def get_filtered_incidents(start_date, end_date, distrito, causa):
    """Construye y ejecuta una query de Flux dinámica basada en los filtros."""
    processed_incidents = []
    try:
        query_api = influx_client.query_api()
        query_parts = [f'from(bucket: "{INFLUXDB_BUCKET}")']

        # ---- Construcción de rango (local -03:00 → UTC) ----
        if start_date or end_date:
            # Normalizar a date, admitiendo date o datetime
            sd = _as_date(start_date) if start_date else date(2000, 1, 1)
            ed = _as_date(end_date)   if end_date   else date.today()
            # Día local completo → UTC
            sd_local = TZ.localize(datetime.combine(sd, time.min))
            ed_local = TZ.localize(datetime.combine(ed, time(23, 59, 59)))

            sd_utc = sd_local.astimezone(pytz.utc).isoformat()
            ed_utc = ed_local.astimezone(pytz.utc).isoformat()
            query_parts.append(f'|> range(start: time(v: "{sd_utc}"), stop: time(v: "{ed_utc}"))')
        else:
            query_parts.append('|> range(start: -5y)')

        # Medición y pivot
        query_parts += [
            '|> filter(fn: (r) => r._measurement == "incidencia_electrica")',
            '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        ]
        if distrito:
            query_parts.append(f'|> filter(fn: (r) => r.distrito == "{distrito}")')
        if causa:
            causa_escaped = causa.replace('\\', '\\\\').replace('"', '\\"')
            query_parts.append(f'|> filter(fn: (r) => r.descripcion_de_la_causa == "{causa_escaped}")')
        query_parts.append('|> sort(columns: ["_time"], desc: true)')
        query = "\n".join(query_parts)
        tables = query_api.query(query, org=INFLUXDB_ORG)
        incidents_data = [record.values for table in tables for record in table.records]
        for incident in incidents_data:
            # Ahora usamos los campos pivotados en lugar de _time
            incident['fecha_inicio_fmt'] = incident.get('fecha_inicio', '')
            incident['hora_inicio_fmt']  = incident.get('hora_inicio', '')
            incident['fecha_fin_fmt']    = incident.get('fecha_fin', '')
            incident['hora_fin_fmt']     = incident.get('hora_fin', '')
            processed_incidents.append(incident)
    except Exception as e:
        print(f"Error al ejecutar la query de filtro: {e}")
    return processed_incidents

@main.route("/api/end_dates")
@login_required
def api_end_dates():
    """Devuelve las fechas válidas para el combo 'Hasta' dado un 'Desde'."""
    frm = request.args.get("from", "").strip()
    d_from = _to_date_any(frm)
    if d_from is None:
        return jsonify({"dates": []})
    raw_all = get_available_dates()  # strings
    all_dt = sorted({ _to_date_any(d) for d in raw_all if _to_date_any(d) })
    # Solo fechas >= desde
    end_dates = [d.strftime("%d-%m-%Y") for d in all_dt if d >= d_from]
    return jsonify({"dates": end_dates})


@main.route('/', methods=['GET', 'POST'])
@login_required
def index():
    distritos, causas = get_filter_options()
    incidents = []
    available_start_dates = []
    available_end_dates = []
    form_data = {}

    # Traer SIEMPRE todas las fechas disponibles para poblar "Desde"
    raw_all_dates = get_available_dates()  # strings con fechas presentes en DB
    all_dt = sorted({
        (_parse_date_flexible(d).date())
        for d in raw_all_dates
        if _parse_date_flexible(d) is not None
    })
    available_start_dates = [dt.strftime("%d-%m-%Y") for dt in all_dt]

    if request.method == 'GET':
        # Al cargar: "Hasta" vacío (el front puede deshabilitar el combo)
        available_end_dates = []
        return render_template(
            'index.html',
            name=current_user.name,
            distritos=distritos,
            causas=causas,
            form_data=form_data,
            incidents=incidents,
            available_start_dates=available_start_dates,
            available_end_dates=available_end_dates
        )

    # ----- POST (filtrado) -----
    form_data = request.form
    sd_str = (form_data.get('start_date') or "").strip()
    ed_str = (form_data.get('end_date') or "").strip()
    distrito = form_data.get('distrito')
    causa = form_data.get('causa')

    sd = (_parse_date_flexible(sd_str).date() if _parse_date_flexible(sd_str) else None) if sd_str else None
    ed = (_parse_date_flexible(ed_str).date() if _parse_date_flexible(ed_str) else None) if ed_str else None

    # Reglas solicitadas:
    # - Si hay "Desde" y NO hay "Hasta": filtrar SOLO ese día (ed = sd)
    # - Si hay ambos: validar ed >= sd
    # - Si hay "Hasta" pero no "Desde": avisar y volver (no debería pasar por UI)
    if sd and not ed:
        ed = sd
    elif sd and ed:
        if ed < sd:
            flash("La fecha 'Hasta' no puede ser anterior a 'Desde'.", "warning")
            # Reconstruir 'Hasta' en base a sd
            available_end_dates = [d.strftime("%d-%m-%Y") for d in all_dt if d >= sd]
            return render_template(
                'index.html',
                name=current_user.name,
                distritos=distritos,
                causas=causas,
                form_data=form_data,
                incidents=[],
                available_start_dates=available_start_dates,
                available_end_dates=available_end_dates
            )
    elif not sd and ed:
        flash("Seleccione primero la fecha 'Desde'.", "warning")
        # "Hasta" vacío porque no hay 'Desde'
        available_end_dates = []
        return render_template(
            'index.html',
            name=current_user.name,
            distritos=distritos,
            causas=causas,
            form_data=form_data,
            incidents=[],
            available_start_dates=available_start_dates,
            available_end_dates=available_end_dates
        )
    else:
        # Sin fechas: devolvemos la vista sin filtrar (comportamiento mínimo)
        available_end_dates = []
        return render_template(
            'index.html',
            name=current_user.name,
            distritos=distritos,
            causas=causas,
            form_data=form_data,
            incidents=[],
            available_start_dates=available_start_dates,
            available_end_dates=available_end_dates
        )

    # Con sd definido, "Hasta" debe listar SOLO fechas >= sd y existentes en DB
    available_end_dates = [d.strftime("%d-%m-%Y") for d in all_dt if d >= sd]

    # Ejecutar filtro (ahora ed SIEMPRE tiene valor si sd tiene valor)
    incidents = get_filtered_incidents(sd, ed, distrito, causa)

    # Orden cronológico robusto
    incidents.sort(key=lambda inc: _parse_datetime_flexible(
        inc.get('fecha_inicio_fmt', '01-01-1900'),
        inc.get('hora_inicio_fmt', '00:00:00')
    ))

    return render_template(
        'index.html',
        name=current_user.name,
        distritos=distritos,
        causas=causas,
        form_data=form_data,
        incidents=incidents,
        available_start_dates=available_start_dates,
        available_end_dates=available_end_dates
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

    incidents = get_filtered_incidents(start_date, end_date, distrito, causa)

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

