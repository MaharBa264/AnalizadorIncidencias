# /analizador/weather_adapter.py
import os
import pandas as pd
from datetime import timedelta
import pytz

from . import (
    WEATHER_INFLUX_BUCKET,
    WEATHER_MEASUREMENT,
    WEATHER_WIND_FIELD,
    WEATHER_HUM_FIELD,
    WEATHER_TEMP_FIELD,
    WEATHER_SITE_TAG_KEY,
)

TZ = pytz.timezone("America/Argentina/San_Luis")

# ---------------------- Helpers de configuración ----------------------

def _fields_enabled():
    """Devuelve sólo los fields configurados (no vacíos)."""
    return [f for f in (WEATHER_WIND_FIELD, WEATHER_TEMP_FIELD, WEATHER_HUM_FIELD) if f]

# ---------------------- CSV de distritos → tag clima ------------------

def load_distrito_tags(csv_path: str):
    """
    Lee `distrito,weather_tag` → dict {distrito: tag} (strings).
    NO normaliza el caso: debe coincidir con como venga el 'distrito' en incidencias.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"No existe el CSV de tags: {csv_path}")
    df = pd.read_csv(csv_path)
    cols = {c.lower(): c for c in df.columns}
    if not {"distrito", "weather_tag"}.issubset(cols):
        raise ValueError("CSV debe tener columnas 'distrito' y 'weather_tag'.")
    df = df.rename(columns={cols["distrito"]: "distrito", cols["weather_tag"]: "weather_tag"})
    out = {}
    for _, r in df.iterrows():
        d = str(r["distrito"]).strip()
        t = str(r["weather_tag"]).strip()
        if d and t:
            out[d] = t
    return out

# ---------------------- Query a Influx de clima -----------------------

def fetch_weather_df(query_api, tag_value: str, start_utc: str, stop_utc: str) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas: _time, _field, _value
    Filtra por:
      - bucket: WEATHER_INFLUX_BUCKET
      - measurement: WEATHER_MEASUREMENT
      - tag key: WEATHER_SITE_TAG_KEY == tag_value  (por ej., equip_grp == 'ETSL')
      - fields presentes (windspeed, temperature y opcionalmente humedad)
    Aplica aggregateWindow 1h mean y elimina NaNs.
    """
    fields = _fields_enabled()
    if not fields:
        return pd.DataFrame()  # nada que pedir si no hay fields configurados

    field_filter = " or ".join([f'r._field == "{f}"' for f in fields])

    flux = f'''
from(bucket: "{WEATHER_INFLUX_BUCKET}")
  |> range(start: {start_utc}, stop: {stop_utc})
  |> filter(fn: (r) => r._measurement == "{WEATHER_MEASUREMENT}")
  |> filter(fn: (r) => r["{WEATHER_SITE_TAG_KEY}"] == "{tag_value}")
  |> filter(fn: (r) => {field_filter})
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> keep(columns: ["_time","_field","_value"])
  |> yield()
'''

    df = query_api.query_data_frame(flux)

    # El cliente puede devolver una lista de DataFrames
    if isinstance(df, list):
        if len(df) == 0:
            return pd.DataFrame()
        df = pd.concat(df, ignore_index=True)

    if df is None or df.empty:
        return pd.DataFrame()

    # Normalizar tipos y limpiar
    df["_time"] = pd.to_datetime(df["_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["_time", "_value"])
    return df

# ---------------------- Métricas por incidencia -----------------------

def compute_metrics_for_incident(df_weather: pd.DataFrame, t0_local, t1_local):
    """
    Calcula:
      - viento_max / viento_prom (sobre ventana de la incidencia)
      - temp_prom                 (idem)
      - humedad_prom              (idem, opcional)
      - humedad_prev_6h          (6 horas previas al inicio, opcional)
    Si algún field no existe/está vacío, devuelve None para esa métrica.
    """
    if df_weather is None or df_weather.empty:
        return {
            "viento_max": None,
            "viento_prom": None,
            "humedad_prom": None,
            "temp_prom": None,
            "humedad_prev_6h": None,
        }

    def _to_utc(dt):
        if getattr(dt, "tzinfo", None) is None:
            return TZ.localize(dt).astimezone(pytz.utc)
        return dt.astimezone(pytz.utc)

    t0_utc = _to_utc(t0_local)
    t1_utc = _to_utc(t1_local)

    mask_win = (df_weather["_time"] >= t0_utc) & (df_weather["_time"] <= t1_utc)
    mask_prev6 = (df_weather["_time"] < t0_utc) & (df_weather["_time"] >= t0_utc - timedelta(hours=6))

    def _max_mean(field):
        if not field:
            return (None, None)
        s = df_weather.loc[mask_win & (df_weather["_field"] == field), "_value"]
        if s.empty:
            return (None, None)
        return (float(s.max()), float(s.mean()))

    def _mean(field, mask):
        if not field:
            return None
        s = df_weather.loc[mask & (df_weather["_field"] == field), "_value"]
        return float(s.mean()) if not s.empty else None

    viento_max, viento_prom = _max_mean(WEATHER_WIND_FIELD)
    temp_prom    = _mean(WEATHER_TEMP_FIELD, mask_win)
    humedad_prom = _mean(WEATHER_HUM_FIELD, mask_win)
    humedad_prev = _mean(WEATHER_HUM_FIELD, mask_prev6)

    return {
        "viento_max": viento_max,
        "viento_prom": viento_prom,
        "humedad_prom": humedad_prom,
        "temp_prom": temp_prom,
        "humedad_prev_6h": humedad_prev,
    }

# ---------------------- Validación de incidencias ---------------------

_REQUIRED_KEYS = ("distrito", "dt_inicio_local", "dt_fin_local")

def _require_keys_for_cross(incidents):
    """
    Falla temprano si alguna incidencia no trae las claves mínimas.
    No 'rellena' distrito: si falta, hay que arreglar la normalización upstream.
    """
    missing = []
    for i, inc in enumerate(incidents):
        lacks = [k for k in _REQUIRED_KEYS
                 if (k not in inc) or (inc[k] is None) or (str(inc[k]).strip() == "")]
        if lacks:
            eg = {k: inc.get(k) for k in _REQUIRED_KEYS}
            missing.append({"idx": i, "missing": lacks, "example": eg})
            if len(missing) >= 3:
                break
    if missing:
        raise ValueError(f"Incidencias sin claves requeridas {_REQUIRED_KEYS}. Ejemplos: {missing}")

# ---------------------- Cruce incidencias ↔ clima ---------------------

def cross_incidents_with_weather(query_api, incidents, distrito_tags):
    """
    - Agrupa incidencias por 'distrito'.
    - Usa CSV 'distrito' → 'weather_tag' para obtener el valor del tag del clima
      (por ej., equip_grp = 'ETSL').
    - Pide al Influx de clima una sola ventana por distrito: [min(dt_inicio)-6h, max(dt_fin)].
    - Calcula métricas por incidencia y arma un DataFrame con los resultados.

    Si el distrito no existe en el CSV, la fila queda con métricas None y _clima="sin_tag".
    """
    _require_keys_for_cross(incidents)

    from collections import defaultdict
    by_d = defaultdict(list)
    for inc in incidents:
        by_d[inc.get("distrito") or ""].append(inc)

    rows = []
    for d, items in by_d.items():
        # Mapear 'distrito' → tag de clima (p.ej. ETSL)
        tag = distrito_tags.get(str(d).strip())

        # Ventana extendida (-6h al inicio más temprano)
        t0 = min(i["dt_inicio_local"] for i in items)
        t1 = max(i["dt_fin_local"]   for i in items)
        start_utc = TZ.localize(t0 - timedelta(hours=6)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        stop_utc  = TZ.localize(t1).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Si no hay tag, no consultamos clima; devolvemos filas con métricas vacías
        if not tag:
            for inc in items:
                row = {**inc}
                row.update({
                    "viento_max": None,
                    "viento_prom": None,
                    "humedad_prom": None,
                    "temp_prom": None,
                    "humedad_prev_6h": None,
                    "_clima": "sin_tag",
                })
                rows.append(row)
            continue

        # Traer clima para ese distrito/tag en una sola query
        dfw = fetch_weather_df(query_api, tag, start_utc, stop_utc)

        # Métricas por incidencia
        for inc in items:
            mets = compute_metrics_for_incident(dfw, inc["dt_inicio_local"], inc["dt_fin_local"])
            row = {**inc}
            row.update(mets)
            row["_clima"] = "ok" if dfw is not None and not dfw.empty else "sin_datos"
            rows.append(row)

    return pd.DataFrame(rows)
