# /analizador/services.py
# Contiene la lógica de negocio, como el procesamiento de archivos.

import os
import pandas as pd
from . import influx_client, INFLUXDB_ORG, INFLUXDB_BUCKET
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

def to_int(value):
    """Convierte un valor a entero de forma segura, devolviendo 0 si falla."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

def process_file_to_influxdb(filepath):
    """Lee un archivo (CSV o Excel), lo normaliza y lo inserta en InfluxDB."""
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
        'cantidad_de_reclamos': ['cantidad_de_reclamos', 'cant_reclamos', 'cantidad de reclamos']
    }

    try:
        filename = os.path.basename(filepath).lower()
        if filename.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='latin1', sep=',', index_col=False)
        elif filename.endswith('.xlsx'):
            df = pd.read_excel(filepath, engine='openpyxl')
        elif filename.endswith('.xls'):
            df = pd.read_excel(filepath, engine='xlrd')
        else:
            print(f"Error: Formato de archivo no soportado: {filename}")
            return

        print(f"Pandas leyó {len(df)} filas. Columnas originales: {list(df.columns)}")
        
        rename_map = {}
        for canonical_name, possible_names in column_mapping.items():
            for name in possible_names:
                if name in df.columns:
                    rename_map[name] = canonical_name
                    break
        
        df.rename(columns=rename_map, inplace=True)
        final_df = df.loc[:, df.columns.isin(column_mapping.keys())].copy()

        if 'fecha_inicio' not in final_df.columns or 'nro_incidencia' not in final_df.columns:
            print("Error Crítico: El archivo debe contener una columna de fecha y una de incidencia.")
            return

        final_df['fecha_inicio'] = pd.to_datetime(final_df['fecha_inicio'], errors='coerce')
        final_df['fecha_fin'] = pd.to_datetime(final_df['fecha_fin'], errors='coerce')
        final_df.dropna(subset=['fecha_inicio'], inplace=True)
        # Separar fecha y hora para uso posterior (formato legible)
        final_df['fecha_inicio_fecha'] = final_df['fecha_inicio'].dt.strftime('%d/%m/%Y')
        final_df['hora_inicio'] = final_df['fecha_inicio'].dt.strftime('%H:%M:%S')
        final_df['fecha_fin_fecha'] = final_df['fecha_fin'].dt.strftime('%d/%m/%Y')
        final_df['hora_fin'] = final_df['fecha_fin'].dt.strftime('%H:%M:%S')
        
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        points = []
        for _, row in final_df.iterrows():
            if pd.isna(row.get("fecha_inicio")):
                print(f"Fila ignorada por falta de fecha de inicio: {row.to_dict()}")
                continue

            if pd.isna(row.get("nro_incidencia")):
                print(f"Fila sin número de incidencia: {row.to_dict()}")
                continue

            try:
                nro = int(row.get("nro_incidencia"))
                if nro == 0:
                    print(f"Fila con nro_incidencia inválido: {row.to_dict()}")
                    continue
            except:
                print(f"Fila con nro_incidencia no convertible a int: {row.to_dict()}")
                continue

            fecha_fin_obj = row.get("fecha_fin")
            fecha_fin_fecha = fecha_fin_obj.strftime('%d/%m/%Y') if pd.notna(fecha_fin_obj) else ""
            hora_fin = fecha_fin_obj.strftime('%H:%M:%S') if pd.notna(fecha_fin_obj) else ""

            point = Point("incidencia_electrica") \
                .tag("distrito", str(row.get("distrito", "N/A"))) \
                .tag("nivel_tension", str(row.get("nivel_tension", "N/A"))) \
                .field("nro_incidencia", nro) \
                .field("fecha_fin_fecha", fecha_fin_fecha) \
                .field("hora_fin", hora_fin) \
                .field("localidad", str(row.get("localidad", ""))) \
                .field("distribuidor", str(row.get("distribuidor", ""))) \
                .field("instalacion", str(row.get("instalacion", ""))) \
                .field("ct_involucrados", to_int(row.get("ct_involucrados"))) \
                .field("nises_involucrados", to_int(row.get("nises_involucrados"))) \
                .field("potencia_involucrada", to_int(row.get("potencia_involucrada"))) \
                .field("descripcion_de_la_causa", str(row.get("descripcion_de_la_causa", ""))) \
                .field("cantidad_de_reclamos", to_int(row.get("cantidad_de_reclamos"))) \
                .field("fecha_inicio_fecha", row['fecha_inicio'].strftime('%d/%m/%Y')) \
                .field("hora_inicio", row['fecha_inicio'].strftime('%H:%M:%S')) \
                .time(row['fecha_inicio'])

            points.append(point)
        
        if points:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
            print(f"Se escribieron {len(points)} puntos en InfluxDB exitosamente.")

    except Exception as e:
        print(f"Error general durante el procesamiento del archivo: {e}")
