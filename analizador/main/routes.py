# /analizador/main/routes.py
# Rutas principales de la aplicación: panel de control, filtros, descargas, etc.

import os
import io
import pandas as pd
import threading
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
        query_parts = [
            f'from(bucket: "{INFLUXDB_BUCKET}")',
            '|> range(start: -5y)',
            '|> filter(fn: (r) => r._measurement == "incidencia_electrica")',
            '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        ]
        if start_date:
            query_parts.append(f'|> filter(fn: (r) => r._time >= {start_date.isoformat()}Z)')
        if end_date:
            end_date_inclusive = end_date.replace(hour=23, minute=59, second=59)
            query_parts.append(f'|> filter(fn: (r) => r._time <= {end_date_inclusive.isoformat()}Z)')
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
            incident['fecha_inicio_fmt'] = incident['_time'].strftime('%d/%m/%Y')
            incident['hora_inicio_fmt'] = incident['_time'].strftime('%H:%M:%S')
            incident['fecha_fin_fmt'] = incident.get('fecha_fin_fecha', '')
            incident['hora_fin_fmt'] = incident.get('hora_fin', '')
            incident['hora_fin_fmt'] = incident.get('hora_fin', '')
            processed_incidents.append(incident)
    except Exception as e:
        print(f"Error al ejecutar la query de filtro: {e}")
    return processed_incidents

@main.route('/', methods=['GET', 'POST'])
@login_required
def index():
    distritos, causas = get_filter_options()
    available_dates = get_available_dates()
    incidents = []
    form_data = {}
    if request.method == 'POST':
        form_data = request.form

        ### INICIO CAMBIO: conversión segura de fechas y fallback
        start_date_str = form_data.get('start_date')
        end_date_str = form_data.get('end_date')

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d') if start_date_str else None
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d') if end_date_str else None

        if start_date and not end_date:
            end_date = start_date + timedelta(days=1)
        
        distrito = form_data.get('distrito')
        causa = form_data.get('causa')
        if start_date and not end_date:
            end_date = start_date + timedelta(days=1)
        incidents = get_filtered_incidents(start_date, end_date, distrito, causa)
    return render_template('index.html',
                            name=current_user.name,
                            incidents=incidents,
                            available_dates=available_dates,
                            form_data=form_data,
                            distritos=distritos,  # ✅ USAR DATOS REALES
                            causas=causas)

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

