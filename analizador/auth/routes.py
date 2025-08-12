# /analizador/auth/routes.py
import io, base64, datetime as dt
from flask import render_template, redirect, url_for, request, flash, current_app, abort, session as flask_session
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, logout_user, login_required, current_user
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
import pyotp
import qrcode

from . import auth
from ..models import User
from .. import db, mail
from ..decorators import admin_required

# ----- Helpers -----
def _serializer():
    secret = current_app.config['SECRET_KEY']
    salt = current_app.config.get('SECURITY_PASSWORD_SALT', 'security-salt')
    return URLSafeTimedSerializer(secret_key=secret, salt=salt)

def _is_locked(user: User) -> bool:
    return bool(user.locked_until and dt.datetime.utcnow() < user.locked_until)

def _record_failed_login(user: User, lock_after=5, lock_minutes=10):
    user.failed_logins = (user.failed_logins or 0) + 1
    if user.failed_logins >= lock_after:
        user.locked_until = dt.datetime.utcnow() + dt.timedelta(minutes=lock_minutes)

def _reset_failed_login(user: User):
    user.failed_logins = 0
    user.locked_until = None

# ----- Auth Routes -----
@auth.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip()
        password = request.form.get('password') or ''
        user = User.query.filter_by(email=email).first()

        if not user or not user.is_active or _is_locked(user) or not check_password_hash(user.password, password):
            if user and not check_password_hash(user.password, password):
                _record_failed_login(user); db.session.commit()
            flash('Por favor, revisá tus datos o esperá a que finalice el bloqueo.')
            return render_template('login.html'), 401

        # password OK
        _reset_failed_login(user); db.session.commit()

        # Si 2FA habilitado -> ir a verificación
        if user.is_2fa_enabled and (user.totp_secret):
            flask_session['pending_2fa_user_id'] = user.id
            return redirect(url_for('auth.verify_2fa'))

        # login directo
        login_user(user, remember=False, duration=current_app.config.get('REMEMBER_COOKIE_DURATION'))
        user.last_login_at = dt.datetime.utcnow(); db.session.commit()
        next_url = request.args.get('next') or url_for('main.index')
        return redirect(next_url)
    return render_template('login.html')

@auth.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))

# ----- 2FA (TOTP) -----
@auth.route('/2fa/setup')
@login_required
def setup_2fa():
    user = User.query.get(current_user.id)
    if not user.totp_secret:
        user.totp_secret = pyotp.random_base32()
        db.session.commit()

    issuer = current_app.config.get('SECURITY_ISSUER', 'Incidencias')
    otp_uri = pyotp.totp.TOTP(user.totp_secret).provisioning_uri(name=user.email, issuer_name=issuer)

    # QR como data URI
    img = qrcode.make(otp_uri)
    buf = io.BytesIO(); img.save(buf, format='PNG')
    data_uri = 'data:image/png;base64,' + base64.b64encode(buf.getvalue()).decode('ascii')

    return render_template('twofa_setup.html', data_uri=data_uri, otp_uri=otp_uri, is_enabled=user.is_2fa_enabled)

@auth.route('/2fa/enable', methods=['POST'])
@login_required
def enable_2fa():
    code = (request.form.get('code') or '').strip()
    user = User.query.get(current_user.id)
    totp = pyotp.TOTP(user.totp_secret)
    if not totp.verify(code, valid_window=1):
        flash('Código 2FA incorrecto.', 'danger')
        return redirect(url_for('auth.setup_2fa'))
    user.is_2fa_enabled = True; db.session.commit()
    flash('2FA habilitado.', 'success')
    return redirect(url_for('auth.setup_2fa'))

@auth.route('/2fa/disable', methods=['POST'])
@login_required
def disable_2fa():
    user = User.query.get(current_user.id)
    user.is_2fa_enabled = False; db.session.commit()
    flash('2FA deshabilitado.', 'info')
    return redirect(url_for('auth.setup_2fa'))

@auth.route('/2fa/verify', methods=['GET', 'POST'])
def verify_2fa():
    pending_id = flask_session.get('pending_2fa_user_id')
    if not pending_id:
        return redirect(url_for('auth.login'))
    if request.method == 'POST':
        code = (request.form.get('code') or '').strip()
        user = User.query.get(int(pending_id))
        if not user or not user.is_2fa_enabled:
            flash('Sesión 2FA inválida.', 'danger')
            return redirect(url_for('auth.login'))
        totp = pyotp.TOTP(user.totp_secret)
        if not totp.verify(code, valid_window=1):
            flash('Código 2FA incorrecto.', 'danger')
            return render_template('twofa_verify.html'), 401
        login_user(user, remember=False)
        user.last_login_at = dt.datetime.utcnow(); db.session.commit()
        flask_session.pop('pending_2fa_user_id', None)
        return redirect(url_for('main.index'))
    return render_template('twofa_verify.html')

# ----- Reset por email -----
def _send_reset_email(to_email: str, url: str):
    try:
        from flask_mail import Message
        msg = Message(subject='Restablecer contraseña', recipients=[to_email], body=f'Usá este enlace: {url}')
        mail.send(msg)
        return True, None
    except Exception as e:
        return False, str(e)

@auth.route('/reset/request', methods=['GET', 'POST'])
def reset_request():
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip()
        user = User.query.filter_by(email=email).first()
        if user:
            token = _serializer().dumps(email)
            reset_url = url_for('auth.reset_with_token', token=token, _external=True)
            ok, err = _send_reset_email(email, reset_url)
            if ok:
                flash('Si el email existe, enviamos un enlace de restablecimiento.', 'info')
            else:
                flash(f'SMTP no configurado: {reset_url}', 'warning')
        else:
            flash('Si el email existe, enviamos un enlace de restablecimiento.', 'info')
    return render_template('reset_request.html')

@auth.route('/reset/<token>', methods=['GET', 'POST'])
def reset_with_token(token):
    try:
        email = _serializer().loads(token, max_age=3600)
    except (BadSignature, SignatureExpired):
        flash('Token inválido o expirado.', 'danger')
        return redirect(url_for('auth.reset_request'))
    if request.method == 'POST':
        password = request.form.get('password') or ''
        confirm = request.form.get('confirm') or ''
        if len(password) < 8 or password != confirm:
            flash('Las contraseñas no coinciden o son débiles.', 'danger')
            return render_template('reset_with_token.html')
        user = User.query.filter_by(email=email).first()
        if not user: 
            flash('Usuario no encontrado.', 'danger')
            return redirect(url_for('auth.reset_request'))
        user.password = generate_password_hash(password, method='pbkdf2:sha256')
        db.session.commit()
        flash('Contraseña cambiada. Ingresá con tu nueva clave.', 'success')
        return redirect(url_for('auth.login'))
    return render_template('reset_with_token.html')

# ----- Alta de usuarios (solo admin) -----
@auth.route('/signup', methods=['GET', 'POST'])
@login_required
@admin_required
def signup():
    if request.method == 'POST':
        name = (request.form.get('name') or '').strip()
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''
        role = (request.form.get('role') or 'visor').strip()
        if role not in ('admin','analista','visor'):
            role = 'visor'
        if not name or not email or len(password) < 8:
            flash('Datos inválidos o contraseña débil.')
            return render_template('signup.html')
        if User.query.filter_by(email=email).first():
            flash('El email ya existe.')
            return render_template('signup.html')
        user = User(name=name, email=email, role=role, password=generate_password_hash(password, method='pbkdf2:sha256'))
        db.session.add(user); db.session.commit()
        flash('Usuario creado.')
        return redirect(url_for('auth.login'))
    return render_template('signup.html')
