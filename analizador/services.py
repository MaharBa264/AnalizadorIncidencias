# /analizador/services.py
# Contiene la lógica de negocio, como el procesamiento de archivos.

import os
import pandas as pd
from . import influx_client, INFLUXDB_ORG, INFLUXDB_BUCKET
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime


def to_int(value):
    """Convierte un valor a entero de forma segura, devolviendo 0 si falla."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

def process_file_to_influxdb(filepath):
    """Lee un archivo CSV de incidencias, lo normaliza y lo inserta en InfluxDB optimizadamente."""
    print(f"\n--- Iniciando procesamiento de {os.path.basename(filepath)} ---")

    column_mapping = {
        'nro_incidencia': ['nro_incidencia', 'incidencia'],
        'fecha_inicio': ['fecha_inicio', 'fecha_de_alta'],
        'fecha_fin': ['fecha_fin', 'fecha_de_reposicion'],
        'distrito': ['distrito'],
        'nivel_tension': ['nivel_tension', 'MT-BT'],
        'localidad': ['localidad'],
        'distribuidor': ['distribuidor'],
        'instalacion': ['instalacion'],
        'ct_involucrados': ['ct_involucrados', 'cantidad_de_ct_afectados'],
        'nises_involucrados': ['nises_involucrados', 'clientes_afectados'],
        'potencia_involucrada': ['potencia_involucrada', 'potencia_instalada'],
        'descripcion_de_la_causa': ['descripcion_de_la_causa'],
        'cantidad_de_reclamos': ['cantidad_de_reclamos', 'cant_reclamos', 'cantidad de reclamos'],
        'extraccion': ['extraccion']
    }

    try:
        filename = os.path.basename(filepath).lower()
        if filename.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='latin1', sep=',', index_col=False)
        else:
            print(f"Error: Solo se permite formato CSV. Archivo: {filename}")
            return

        print(f"Pandas leyó {len(df)} filas.")

        # Normalizar nombres de columnas
        new_columns = {}
        for std_name, aliases in column_mapping.items():
            for alias in aliases:
                if alias in df.columns:
                    new_columns[alias] = std_name
                    break
        df = df.rename(columns=new_columns)

        # Eliminar filas sin fecha de inicio
        df = df.dropna(subset=['fecha_inicio'])

        # Separar fecha y hora
        def parse_fecha_hora(col):
            fecha = pd.to_datetime(df[col], format='%Y%m%d %H:%M:%S', errors='coerce')
            df[f'{col}_fecha'] = fecha.dt.strftime('%d-%m-%Y')
            df[f'{col}_hora'] = fecha.dt.strftime('%H:%M:%S')

        if 'fecha_inicio' in df.columns:
            parse_fecha_hora('fecha_inicio')
        if 'fecha_fin' in df.columns:
            parse_fecha_hora('fecha_fin')

        df.drop(columns=['fecha_inicio', 'fecha_fin'], inplace=True, errors='ignore')

        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        batch = []
        for i, row in enumerate(df.itertuples(index=False), 1):
            try:
                point = Point("incidencia_electrica") \
                    .tag("nivel_tension", str(getattr(row, "nivel_tension", ""))) \
                    .tag("distrito", str(getattr(row, "distrito", ""))) \
                    .tag("localidad", str(getattr(row, "localidad", ""))) \
                    .tag("distribuidor", str(getattr(row, "distribuidor", ""))) \
                    .tag("instalacion", str(getattr(row, "instalacion", ""))) \
                    .tag("descripcion_de_la_causa", str(getattr(row, "descripcion_de_la_causa", ""))) \
                    .field("ct_involucrados", int(getattr(row, "ct_involucrados", 0))) \
                    .field("nises_involucrados", int(getattr(row, "nises_involucrados", 0))) \
                    .field("potencia_involucrada", float(getattr(row, "potencia_involucrada", 0))) \
                    .field("cantidad_de_reclamos", int(getattr(row, "cantidad_de_reclamos", 0))) \
                    .field("nro_incidencia", str(getattr(row, "nro_incidencia", ""))) \
                    .field("extraccion", str(getattr(row, "extraccion", ""))) \
                    .field("fecha_inicio", str(getattr(row, "fecha_inicio_fecha", ""))) \
                    .field("hora_inicio", str(getattr(row, "fecha_inicio_hora", ""))) \
                    .field("fecha_fin", str(getattr(row, "fecha_fin_fecha", ""))) \
                    .field("hora_fin", str(getattr(row, "fecha_fin_hora", ""))) \
                    .field("indice", i) \
                    .time(datetime.utcnow())
                batch.append(point)

                if len(batch) >= 500:
                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=batch)
                    batch.clear()
            except Exception as e:
                print(f"Error procesando fila {i}: {e}")

        if batch:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=batch)

        print(f"✓ Se insertaron {len(df)} puntos en InfluxDB.")
    except Exception as e:
        print(f"❌ Error general procesando archivo: {e}")
