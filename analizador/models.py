# /analizador/models.py
# Define los modelos de la base de datos (SQLAlchemy).

from flask_login import UserMixin
from . import db

class User(UserMixin, db.Model):
    """Modelo para la tabla de usuarios."""
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(100), nullable=False)

    # Roles: admin / analista / visor
    role = db.Column(db.String(50), nullable=False, default='visor')

    # Estado y auditoría
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    failed_logins = db.Column(db.Integer, default=0, nullable=False)
    locked_until = db.Column(db.DateTime, nullable=True)
    last_login_at = db.Column(db.DateTime, nullable=True)

    # 2FA (TOTP)
    totp_secret = db.Column(db.String(64), nullable=True)
    is_2fa_enabled = db.Column(db.Boolean, default=False, nullable=False)
