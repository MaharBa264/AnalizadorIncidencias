# /analizador/__init__.py
# Este archivo inicializa la aplicación Flask y sus extensiones (Application Factory).

import os
from flask import Flask
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_mail import Mail

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# --- Base de Datos de Usuarios (SQLAlchemy) ---
db = SQLAlchemy()

# Mail para reset de contraseña
mail = Mail()

# --- Configuración de InfluxDB ---
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')


# Cliente global de InfluxDB
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, timeout=60_000, enable_gzip=True)

# === InfluxDB de Clima (REMOTO/PRODUCCIÓN) ===
WEATHER_INFLUX_URL = os.getenv('WEATHER_INFLUX_URL') or INFLUXDB_URL
WEATHER_INFLUX_TOKEN = os.getenv('WEATHER_INFLUX_TOKEN') or INFLUXDB_TOKEN
WEATHER_INFLUX_ORG = os.getenv('WEATHER_INFLUX_ORG') or INFLUXDB_ORG
WEATHER_INFLUX_BUCKET = os.getenv('WEATHER_INFLUX_BUCKET', 'weather')

WEATHER_MEASUREMENT  = os.getenv('WEATHER_MEASUREMENT', 'weather_hourly')
WEATHER_WIND_FIELD   = os.getenv('WEATHER_WIND_FIELD', 'windspeed')
WEATHER_HUM_FIELD    = os.getenv('WEATHER_HUM_FIELD', 'relative_humidity')
WEATHER_TEMP_FIELD   = os.getenv('WEATHER_TEMP_FIELD', 'temperature')
WEATHER_SITE_TAG_KEY = os.getenv('WEATHER_SITE_TAG_KEY', 'site_tag')

weather_influx_client = InfluxDBClient(
    url=WEATHER_INFLUX_URL,
    token=WEATHER_INFLUX_TOKEN,
    org=WEATHER_INFLUX_ORG,
    timeout=60_000,
    enable_gzip=True,
)

def setup_influxdb():
    """Comprueba si el bucket de InfluxDB existe y lo crea si es necesario."""
    try:
        bucket_api = influx_client.buckets_api()
        bucket = bucket_api.find_bucket_by_name(INFLUXDB_BUCKET)
        if not bucket:
            print(f"Bucket '{INFLUXDB_BUCKET}' no encontrado. Creándolo...")
            bucket_api.create_bucket(bucket_name=INFLUXDB_BUCKET, org=INFLUXDB_ORG)
        else:
            print(f"Bucket '{INFLUXDB_BUCKET}' ya existe.")
    except Exception as e:
        print(f"Error fatal al configurar InfluxDB: {e}")

def create_app():
    """Crea y configura la instancia de la aplicación Flask."""
    app = Flask(__name__)
    
    app.config['SECRET_KEY'] = 'clave_secreta_muy_segura_cambiar_en_produccion'
    # Seguridad cookies
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    # Mail (leer de .env si existen)
    app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER')
    app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', '587'))
    app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'true').lower()=='true'
    app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
    app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
    app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER', 'no-reply@example.com')
    
    # Asegurarse de que la carpeta 'instance' exista para la base de datos SQLite
    try:
        os.makedirs(app.instance_path, exist_ok=True)
    except OSError:
        pass

    # Configuración de la base de datos de usuarios
    db_path = os.path.join(app.instance_path, 'users.db')
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    db.init_app(app)

    # Inicializar Mail
    mail.init_app(app)

    # Configuración del gestor de sesiones de usuario
    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)

    from .models import User

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # Configurar la carpeta de subidas de archivos
    upload_path = os.path.join(os.path.dirname(app.instance_path), 'uploads')
    os.makedirs(upload_path, exist_ok=True)
    app.config['UPLOAD_FOLDER'] = upload_path

    # Verificar configuración de InfluxDB al iniciar
    if all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET]):
        setup_influxdb()

    # Registrar Blueprints (módulos de la aplicación)
    from .main.routes import main as main_blueprint
    app.register_blueprint(main_blueprint)

    from .auth.routes import auth as auth_blueprint
    app.register_blueprint(auth_blueprint)
    
    # Crear las tablas de la base de datos de usuarios si no existen
    with app.app_context():
        db.create_all()

    return app
