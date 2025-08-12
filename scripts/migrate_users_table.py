# /scripts/migrate_users_table.py
# Ejecutar con el venv de la app: python -m scripts.migrate_users_table

import os
import sqlite3

def column_exists(cur, table, column):
    cur.execute(f"PRAGMA table_info({table});")
    return any(row[1] == column for row in cur.fetchall())

def run(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Añadir columnas si faltan
    ops = []
    def addcol(name, type_):
        ops.append((name, type_))
    addcol("role", "TEXT DEFAULT 'visor' NOT NULL")
    addcol("is_active", "INTEGER DEFAULT 1 NOT NULL")
    addcol("failed_logins", "INTEGER DEFAULT 0 NOT NULL")
    addcol("locked_until", "DATETIME NULL")
    addcol("last_login_at", "DATETIME NULL")
    addcol("totp_secret", "TEXT NULL")
    addcol("is_2fa_enabled", "INTEGER DEFAULT 0 NOT NULL")

    for name, type_ in ops:
        if not column_exists(cur, "user", name) and not column_exists(cur, "users", name):
            try:
                cur.execute(f"ALTER TABLE users ADD COLUMN {name} {type_};")
                print(f"Agregada columna: {name}")
            except Exception as e:
                print(f"No se pudo agregar {name}: {e}")
    conn.commit(); conn.close()

if __name__ == '__main__':
    # Detectar ruta por defecto (instance/security.sqlite3 no; usamos la DB de la app)
    db_path = os.environ.get('APP_SQLITE_PATH') or os.path.join(os.path.dirname(__file__), '..', 'instance', 'app.sqlite3')
    db_path = os.path.abspath(db_path)
    print(f"Usando DB: {db_path}")
    run(db_path)
