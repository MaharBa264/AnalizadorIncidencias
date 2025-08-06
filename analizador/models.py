# /analizador/models.py
# Define los modelos de la base de datos (SQLAlchemy).

from flask_login import UserMixin
from . import db

class User(UserMixin, db.Model):
    """Modelo para la tabla de usuarios."""
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    role = db.Column(db.String(50), nullable=False, default='user') # Roles: 'user' o 'admin'
