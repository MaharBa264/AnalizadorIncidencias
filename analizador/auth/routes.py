# /analizador/auth/routes.py
from flask import render_template, redirect, url_for, request, flash
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, logout_user, login_required
from . import auth
from ..models import User
from .. import db

@auth.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        user = User.query.filter_by(email=email).first()
        if not user or not check_password_hash(user.password, password):
            flash('Por favor, revisa tus datos de acceso y vuelve a intentarlo.')
            return redirect(url_for('auth.login'))
        login_user(user, remember=True)
        return redirect(url_for('main.index'))
    return render_template('login.html')

@auth.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        email = request.form.get('email')
        name = request.form.get('name')
        password = request.form.get('password')
        user = User.query.filter_by(email=email).first()
        if user:
            flash('El correo electrónico ya existe.')
            return redirect(url_for('auth.signup'))
        role = 'admin' if User.query.count() == 0 else 'user'
        if role == 'admin':
            flash('¡Cuenta de administrador creada exitosamente!')
        new_user = User(
            email=email, 
            name=name, 
            password=generate_password_hash(password, method='pbkdf2:sha256'),
            role=role
        )
        db.session.add(new_user)
        db.session.commit()
        return redirect(url_for('auth.login'))
    return render_template('signup.html')

@auth.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('main.index'))
