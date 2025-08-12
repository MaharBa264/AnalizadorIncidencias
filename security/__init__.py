import os
import datetime as dt
from flask import current_app
from flask_login import LoginManager
from flask_wtf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_mail import Mail

# Globals initialized on init_app
login_manager = LoginManager()
csrf = CSRFProtect()
limiter = None
mail = Mail()

# SQLAlchemy (vanilla)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base

Base = declarative_base()
engine = None
Session = None

def get_db():
    global Session
    if Session is None:
        raise RuntimeError("Security DB not initialized. Call security.init_app(app) first.")
    return Session()

def init_db(app):
    global engine, Session
    uri = app.config.get("SECURITY_DATABASE_URI")
    if not uri:
        # default to a local sqlite file inside instance folder
        instance_path = app.instance_path if app.instance_path else os.path.dirname(app.root_path)
        os.makedirs(instance_path, exist_ok=True)
        uri = "sqlite:///" + os.path.join(instance_path, "security.sqlite3")
        app.config["SECURITY_DATABASE_URI"] = uri

    connect_args = {}
    if uri.startswith("sqlite:///"):
        connect_args["check_same_thread"] = False

    engine = create_engine(uri, connect_args=connect_args, pool_pre_ping=True)
    Session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))
    from .models import User  # ensure models are imported so tables are known
    Base.metadata.create_all(engine)

def init_app(app):
    global limiter
    # Secrets / defaults
    app.config.setdefault("SECRET_KEY", os.environ.get("SECRET_KEY", "change-this-in-prod"))
    app.config.setdefault("SECURITY_PASSWORD_SALT", os.environ.get("SECURITY_PASSWORD_SALT", "salt-change-me"))
    app.config.setdefault("SECURITY_ISSUER", os.environ.get("SECURITY_ISSUER", "Incidencias"))
    app.config.setdefault("REMEMBER_COOKIE_DURATION", dt.timedelta(days=7))
    app.config.setdefault("PERMANENT_SESSION_LIFETIME", dt.timedelta(minutes=30))  # inactivity
    app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
    app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
    # If you run behind HTTPS, set this to True
    app.config.setdefault("SESSION_COOKIE_SECURE", False)

    # Rate limiting
    limiter = Limiter(get_remote_address, app=app, default_limits=[])

    # CSRF
    csrf.init_app(app)

    # Mail
    mail.init_app(app)

    # DB
    init_db(app)

    # Login
    login_manager.init_app(app)
    login_manager.login_view = "security.login"

    from .models import User
    @login_manager.user_loader
    def load_user(user_id):
        session = get_db()
        try:
            return session.get(User, int(user_id))
        finally:
            session.close()

    # Register blueprint
    from .routes import bp as security_bp
    app.register_blueprint(security_bp)
