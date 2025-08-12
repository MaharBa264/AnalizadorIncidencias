import datetime as dt
import io
import base64
import pyotp
import qrcode
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, abort
from flask_login import login_user, logout_user, current_user, login_required
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from werkzeug.urls import url_parse

from . import get_db
from .models import User
from .forms import (LoginForm, TwoFAForm, CreateUserForm, UpdateUserForm,
                    ChangePasswordForm, ResetRequestForm, ResetPasswordForm)
from .utils import admin_required
from .email import generate_reset_token, verify_reset_token, send_reset_email

bp = Blueprint("security", __name__, url_prefix="")

# ------------- Helpers -------------
def is_locked(user: User) -> bool:
    return bool(user.locked_until and dt.datetime.utcnow() < user.locked_until)

def record_failed_login(user: User, lock_after=5, lock_minutes=10):
    user.failed_logins = (user.failed_logins or 0) + 1
    if user.failed_logins >= lock_after:
        user.locked_until = dt.datetime.utcnow() + dt.timedelta(minutes=lock_minutes)

def reset_failed_login(user: User):
    user.failed_logins = 0
    user.locked_until = None

# ------------- Routes -------------

@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("main.index") if "main.index" in current_app.view_functions else "/")
    form = LoginForm()
    if form.validate_on_submit():
        session = get_db()
        try:
            user = session.execute(select(User).where(User.username == form.username.data)).scalar_one_or_none()
            if not user or not user.is_active:
                flash("Usuario/contraseña inválidos.", "danger")
                return render_template("security/login.html", form=form), 401

            if is_locked(user):
                flash("Usuario bloqueado por múltiples intentos. Probá más tarde.", "danger")
                return render_template("security/login.html", form=form), 403

            if not user.check_password(form.password.data):
                record_failed_login(user)
                session.commit()
                flash("Usuario/contraseña inválidos.", "danger")
                return render_template("security/login.html", form=form), 401

            # Password ok
            reset_failed_login(user)
            session.commit()

            # 2FA flow
            if user.is_2fa_enabled:
                # Store pending user id in session (server-side session handled by Flask)
                from flask import session as flask_session
                flask_session["pending_2fa_user_id"] = user.id
                return redirect(url_for("security.verify_2fa"))
            else:
                login_user(user, remember=form.remember.data, duration=current_app.config.get("REMEMBER_COOKIE_DURATION"))
                user.last_login_at = dt.datetime.utcnow()
                session.commit()
                next_page = request.args.get("next")
                if not next_page or url_parse(next_page).netloc != "":
                    next_page = url_for("main.index") if "main.index" in current_app.view_functions else "/"
                return redirect(next_page)
        finally:
            session.close()
    return render_template("security/login.html", form=form)

@bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("security.login"))

@bp.route("/2fa/setup", methods=["GET", "POST"])
@login_required
def setup_2fa():
    session = get_db()
    try:
        user = session.get(User, current_user.id)
        if request.method == "POST":
            # enable / disable handled via separate endpoints for clarity
            pass

        if not user.totp_secret:
            user.totp_secret = pyotp.random_base32()
            session.commit()

        issuer = current_app.config.get("SECURITY_ISSUER", "Incidencias")
        otp_uri = pyotp.totp.TOTP(user.totp_secret).provisioning_uri(name=user.email, issuer_name=issuer)

        # QR as data URI
        img = qrcode.make(otp_uri)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        data_uri = "data:image/png;base64," + b64

        return render_template("security/setup_2fa.html", otp_uri=otp_uri, data_uri=data_uri, is_enabled=user.is_2fa_enabled)
    finally:
        session.close()

@bp.route("/2fa/enable", methods=["POST"])
@login_required
def enable_2fa():
    form = TwoFAForm()
    if not form.validate_on_submit():
        flash("Código inválido.", "danger")
        return redirect(url_for("security.setup_2fa"))
    session = get_db()
    try:
        user = session.get(User, current_user.id)
        totp = pyotp.TOTP(user.totp_secret)
        if not totp.verify(form.code.data, valid_window=1):
            flash("Código 2FA incorrecto.", "danger")
            return redirect(url_for("security.setup_2fa"))
        user.is_2fa_enabled = True
        session.commit()
        flash("2FA habilitado.", "success")
        return redirect(url_for("security.setup_2fa"))
    finally:
        session.close()

@bp.route("/2fa/disable", methods=["POST"])
@login_required
def disable_2fa():
    session = get_db()
    try:
        user = session.get(User, current_user.id)
        user.is_2fa_enabled = False
        session.commit()
        flash("2FA deshabilitado.", "info")
        return redirect(url_for("security.setup_2fa"))
    finally:
        session.close()

@bp.route("/2fa/verify", methods=["GET", "POST"])
def verify_2fa():
    from flask import session as flask_session
    pending_id = flask_session.get("pending_2fa_user_id")
    if not pending_id:
        return redirect(url_for("security.login"))
    form = TwoFAForm()
    if form.validate_on_submit():
        session = get_db()
        try:
            user = session.get(User, int(pending_id))
            if not user or not user.is_2fa_enabled:
                flash("Sesión inválida.", "danger")
                return redirect(url_for("security.login"))
            totp = pyotp.TOTP(user.totp_secret)
            if not totp.verify(form.code.data, valid_window=1):
                flash("Código 2FA incorrecto.", "danger")
                return render_template("security/verify_2fa.html", form=form), 401
            # ok
            login_user(user, remember=False)
            user.last_login_at = dt.datetime.utcnow()
            session.commit()
            flask_session.pop("pending_2fa_user_id", None)
            next_page = url_for("main.index") if "main.index" in current_app.view_functions else "/"
            return redirect(next_page)
        finally:
            session.close()
    return render_template("security/verify_2fa.html", form=form)

# -------- Admin users --------

@bp.route("/admin/users")
@admin_required
def users_list():
    session = get_db()
    try:
        users = session.execute(select(User)).scalars().all()
        return render_template("security/users_list.html", users=users)
    finally:
        session.close()

@bp.route("/admin/users/new", methods=["GET", "POST"])
@admin_required
def users_new():
    form = CreateUserForm()
    if form.validate_on_submit():
        session = get_db()
        try:
            u = User(username=form.username.data.strip(),
                     email=form.email.data.strip(),
                     role=form.role.data,
                     is_active=form.is_active.data)
            u.set_password(form.password.data)
            session.add(u)
            session.commit()
            flash("Usuario creado.", "success")
            return redirect(url_for("security.users_list"))
        except IntegrityError:
            session.rollback()
            flash("Usuario o email ya existen.", "danger")
        finally:
            session.close()
    return render_template("security/users_new.html", form=form)

@bp.route("/admin/users/<int:user_id>/edit", methods=["GET", "POST"])
@admin_required
def users_edit(user_id):
    session = get_db()
    try:
        u = session.get(User, user_id)
        if not u:
            abort(404)
        form = UpdateUserForm(obj=u)
        if form.validate_on_submit():
            u.email = form.email.data.strip()
            u.role = form.role.data
            u.is_active = form.is_active.data
            session.commit()
            flash("Usuario actualizado.", "success")
            return redirect(url_for("security.users_list"))
        return render_template("security/users_edit.html", form=form, user=u)
    finally:
        session.close()

@bp.route("/admin/users/<int:user_id>/password", methods=["POST"])
@admin_required
def users_reset_password(user_id):
    # Admin forces password change (sets a temporary)
    session = get_db()
    try:
        u = session.get(User, user_id)
        if not u:
            abort(404)
        tmp = pyotp.random_base32()[:12]
        u.set_password(tmp)
        session.commit()
        flash(f"Contraseña temporal: {tmp}", "info")
        return redirect(url_for("security.users_list"))
    finally:
        session.close()

# -------- Password reset by email --------

@bp.route("/reset/request", methods=["GET", "POST"])
def reset_request():
    form = ResetRequestForm()
    if form.validate_on_submit():
        session = get_db()
        try:
            user = session.execute(select(User).where(User.email == form.email.data.strip())).scalar_one_or_none()
            if user:
                token = generate_reset_token(user.email)
                reset_url = url_for("security.reset_with_token", token=token, _external=True)
                try:
                    send_reset_email(user.email, reset_url)
                    flash("Se envió un email con instrucciones si el correo existe.", "info")
                except Exception as e:
                    # fallback: mostrar link (solo si no hay SMTP configurado)
                    flash(f"SMTP no configurado. Usá este enlace manualmente: {reset_url}", "warning")
            else:
                flash("Se envió un email con instrucciones si el correo existe.", "info")
        finally:
            session.close()
    return render_template("security/reset_request.html", form=form)

@bp.route("/reset/<token>", methods=["GET", "POST"])
def reset_with_token(token):
    form = ResetPasswordForm()
    email = None
    try:
        email = verify_reset_token(token)
    except Exception:
        flash("Token inválido o expirado.", "danger")
        return redirect(url_for("security.reset_request"))
    if form.validate_on_submit():
        session = get_db()
        try:
            user = session.execute(select(User).where(User.email == email)).scalar_one_or_none()
            if not user:
                flash("Usuario no encontrado.", "danger")
                return redirect(url_for("security.reset_request"))
            user.set_password(form.password.data)
            session.commit()
            flash("Contraseña restablecida. Ingresá con tu nueva clave.", "success")
            return redirect(url_for("security.login"))
        finally:
            session.close()
    return render_template("security/reset_with_token.html", form=form)
