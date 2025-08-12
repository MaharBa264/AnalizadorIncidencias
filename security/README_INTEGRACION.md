# Seguridad (MVP robusto) – Integración rápida

Este módulo agrega:
- Usuarios y roles (`admin`, `analista`, `visor`)
- Inicio de sesión con bloqueo por intentos
- CSRF (Flask-WTF)
- Rate-limiting en vistas (ya aplicable a /login si querés envolverla)
- 2FA (TOTP) con QR (pyotp + qrcode): `/2fa/setup`
- Restablecimiento de contraseña por email: `/reset/request`

## Requisitos
Python 3.10+ recomendado.
Agregá a tu `requirements.txt` (o instalá con pip):
```
Flask-WTF
Flask-Login
Flask-Limiter
Flask-Mail
itsdangerous
SQLAlchemy>=1.4
pyotp
qrcode[pil]
```

## Configuración mínima (en tu app Flask)

En tu `__init__.py` (o donde creás `app`):

```python
from security import init_app as security_init

def create_app():
    app = Flask(__name__)
    # Claves/seguridad
    app.config["SECRET_KEY"] = "cambiame"
    app.config["SECURITY_PASSWORD_SALT"] = "cambiame-salt"
    app.config["SECURITY_ISSUER"] = "Incidencias"
    # DB para usuarios (opcional; por defecto crea instance/security.sqlite3)
    # app.config["SECURITY_DATABASE_URI"] = "sqlite:////var/lib/analizador/security.sqlite3"

    # Email (para reset)
    app.config.update(
        MAIL_SERVER="smtp.tu-servidor.local",
        MAIL_PORT=587,
        MAIL_USE_TLS=True,
        MAIL_USERNAME="no-reply@tu-dominio",
        MAIL_PASSWORD="tu-clave",
        MAIL_DEFAULT_SENDER=("Incidencias", "no-reply@tu-dominio"),
    )

    # Endurecer sesión
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    # En producción con HTTPS:
    # app.config["SESSION_COOKIE_SECURE"] = True

    security_init(app)  # registra blueprint y crea tablas

    # Opcional: crear admin si no hay usuarios
    from security import get_db
    from security.models import User
    s = get_db()
    try:
        if not s.query(User).first():
            u = User(username="admin", email="admin@example.com", role="admin", is_active=True)
            u.set_password("Cambiar123!")
            s.add(u); s.commit()
            print("Usuario admin creado (Cambiar123!)")
    finally:
        s.close()

    return app
```

## Rutas
- `/login`, `/logout`
- `/admin/users`, `/admin/users/new`, `/admin/users/<id>/edit`
- `/admin/users/<id>/password` (clave temporal)
- `/2fa/setup`, `/2fa/enable`, `/2fa/disable`, `/2fa/verify`
- `/reset/request`, `/reset/<token>`

## Añadir protección por rol a vistas existentes
```python
from security.utils import admin_required, roles_required

@main.route("/admin/panel")
@admin_required
def panel_admin():
    ...
```

## Notas
- El módulo usa SQLite por defecto (archivo `instance/security.sqlite3`). Podés apuntarlo a PostgreSQL/MySQL poniendo `SECURITY_DATABASE_URI`.
- Si usás un `base.html`, las plantillas se heredan de ahí. Si no, copiá el HTML y quitá `{% extends "base.html" %}`.
- Si querés forzar HTTPS y cookies seguras, activá `SESSION_COOKIE_SECURE=True` y configurá proxy headers en tu reverse proxy.
- Para limitar `/login`, podés envolver la vista con `@limiter.limit("5 per minute")` modificando `security/routes.py` si lo necesitás.
