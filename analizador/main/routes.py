# /analizador/main/routes.py
# Rutas principales de la aplicación: panel de control, filtros, descargas, etc.

import os
import io
import pandas as pd
import threading
import pytz
from datetime import datetime, timedelta
from flask import render_template, request, redirect, url_for, jsonify, current_app, send_file, flash
from flask_login import login_required, current_user
from . import main
from .. import influx_client, INFLUXDB_ORG, INFLUXDB_BUCKET
from ..services import process_file_to_influxdb
from ..decorators import admin_required


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
            ar_tz = pytz.timezone("America/Argentina/San_Luis")
            if start_date:
                sd_local = ar_tz.localize(datetime.combine(start_date.date(), datetime.min.time()))
            else:
                # muy atrás si no se especifica (ajustable)
                sd_local = ar_tz.localize(datetime(2000, 1, 1, 0, 0, 0))

            if end_date:
                # fin de día 23:59:59
                ed_local = ar_tz.localize(datetime.combine(end_date.date(), datetime.max.time().replace(microsecond=0)))
            else:
                today_local = ar_tz.localize(datetime.combine(datetime.now().date(), datetime.max.time().replace(microsecond=0)))
                ed_local = today_local

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

@main.route('/', methods=['GET', 'POST'])
@login_required
def index():
    distritos, causas = get_filter_options()
    incidents = []
    available_start_dates = []
    available_end_dates   = []
    form_data = {}

    # ———————————————————————————————
    # 1) GET inicial: sólo cargos las fechas para los selects
    # ———————————————————————————————
    if request.method == 'GET':
        # 1.1) Traigo todas las fechas únicas (YYYY-MM-DD) desde InfluxDB
        raw_dates = get_available_dates()  
        # 1.2) Convierto a datetime y ordeno
        dates_dt = [datetime.strptime(d, "%Y-%m-%d") for d in raw_dates]
        dates_dt.sort()
        # 1.3) Formateo para mostrar en el <select> como DD-MM-YYYY
        available_start_dates = [dt.strftime("%d-%m-%Y") for dt in dates_dt]
        available_end_dates   = available_start_dates[:]

    # ———————————————————————————————
    # 2) POST con filtros: filtro, ordeno y reconstruyo los selects
    # ———————————————————————————————
    else:
        form_data   = request.form
        sd_str      = form_data.get('start_date', "")   # 'DD-MM-YYYY'
        ed_str      = form_data.get('end_date', "")
        distrito    = form_data.get('distrito')
        causa       = form_data.get('causa')

        # 2.1) Parseo a datetime (Python 3.6 no necesita fromisoformat)
        sd = datetime.strptime(sd_str, "%d-%m-%Y") if sd_str else None
        ed = datetime.strptime(ed_str, "%d-%m-%Y") if ed_str else None

        # 2.2) Si solo puso Fecha Desde, asumimos hasta fin de día siguiente
        if sd and not ed:
            ed = sd + timedelta(days=1)

        # 2.3) Llamada real a InfluxDB con objetos datetime
        incidents = get_filtered_incidents(sd, ed, distrito, causa)

        # 2.4) Ordeno cronológicamente (fecha + hora)
        incidents.sort(key=lambda inc: datetime.strptime(
            f"{inc['fecha_inicio_fmt']} {inc['hora_inicio_fmt']}",
            "%d-%m-%Y %H:%M:%S"
        ))

        # 2.5) Reconstruyo los selects de fechas según el resultado
        #      Extraigo fechas únicas de los incidentes, las ordeno y formateo
        dates_dt = sorted({
            datetime.strptime(inc['fecha_inicio_fmt'], "%d-%m-%Y")
            for inc in incidents
        })
        available_start_dates = [dt.strftime("%d-%m-%Y") for dt in dates_dt]

        if sd:
            # Fecha Hasta >= Fecha Desde
            available_end_dates = [
                dt.strftime("%d-%m-%Y")
                for dt in dates_dt
                if dt >= sd
            ]
        else:
            available_end_dates = available_start_dates[:]

    # ———————————————————————————————
    # 3) Renderizado final
    # ———————————————————————————————
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
@main.route('/filtros_opciones')
@login_required
def filtros_opciones():
    distritos, causas = get_filter_options()
    return jsonify({'distritos': distritos, 'causas': causas})


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
    start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d') if request.args.get('start_date') else None
    end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d') if request.args.get('end_date') else None
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

