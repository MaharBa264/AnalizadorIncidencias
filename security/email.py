from itsdangerous import URLSafeTimedSerializer
from flask import current_app
from flask_mail import Message
from . import mail

def _serializer():
    secret = current_app.config["SECRET_KEY"]
    salt = current_app.config["SECURITY_PASSWORD_SALT"]
    return URLSafeTimedSerializer(secret_key=secret, salt=salt)

def generate_reset_token(email: str) -> str:
    s = _serializer()
    return s.dumps(email)

def verify_reset_token(token: str, max_age=3600) -> str:
    s = _serializer()
    return s.loads(token, max_age=max_age)

def send_reset_email(to_email: str, reset_url: str):
    msg = Message(subject="Restablecer contraseña",
                  recipients=[to_email],
                  body=f"Para restablecer tu contraseña, abrí este enlace:\n\n{reset_url}\n\nSi no lo solicitaste, ignorá este email.")
    mail.send(msg)
