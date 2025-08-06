# /analizador/main/__init__.py
# Inicializa el Blueprint principal de la aplicación.

from flask import Blueprint

main = Blueprint('main', __name__)

from . import routes
