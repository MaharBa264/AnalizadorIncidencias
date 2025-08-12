import datetime as dt
import secrets
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import validates
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin

from . import Base

class User(Base, UserMixin):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(32), nullable=False, default="visor")  # admin / analista / visor
    is_active = Column(Boolean, default=True)

    failed_logins = Column(Integer, default=0)
    locked_until = Column(DateTime, nullable=True)

    # 2FA
    totp_secret = Column(String(64), nullable=True)
    is_2fa_enabled = Column(Boolean, default=False)

    created_at = Column(DateTime, default=dt.datetime.utcnow)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow)
    last_login_at = Column(DateTime, nullable=True)

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def get_id(self):
        return str(self.id)

    @validates("role")
    def validate_role(self, key, value):
        if value not in ("admin", "analista", "visor"):
            raise ValueError("Rol inválido")
        return value
