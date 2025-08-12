from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField, SelectField
from wtforms.validators import DataRequired, Length, Email, EqualTo, Optional

class LoginForm(FlaskForm):
    username = StringField("Usuario", validators=[DataRequired(), Length(max=64)])
    password = PasswordField("Contraseña", validators=[DataRequired()])
    remember = BooleanField("Recordarme")
    submit = SubmitField("Ingresar")

class TwoFAForm(FlaskForm):
    code = StringField("Código 2FA", validators=[DataRequired(), Length(min=6, max=6)])
    submit = SubmitField("Verificar")

class CreateUserForm(FlaskForm):
    username = StringField("Usuario", validators=[DataRequired(), Length(max=64)])
    email = StringField("Email", validators=[DataRequired(), Email(), Length(max=255)])
    password = PasswordField("Contraseña", validators=[DataRequired(), Length(min=8)])
    role = SelectField("Rol", choices=[("admin","admin"),("analista","analista"),("visor","visor")])
    is_active = BooleanField("Activo", default=True)
    submit = SubmitField("Crear")

class UpdateUserForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Email(), Length(max=255)])
    role = SelectField("Rol", choices=[("admin","admin"),("analista","analista"),("visor","visor")])
    is_active = BooleanField("Activo")
    submit = SubmitField("Guardar cambios")

class ChangePasswordForm(FlaskForm):
    password = PasswordField("Nueva contraseña", validators=[DataRequired(), Length(min=8)])
    confirm = PasswordField("Confirmar", validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField("Cambiar contraseña")

class ResetRequestForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Email()])
    submit = SubmitField("Enviar enlace de restablecimiento")

class ResetPasswordForm(FlaskForm):
    password = PasswordField("Nueva contraseña", validators=[DataRequired(), Length(min=8)])
    confirm = PasswordField("Confirmar", validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField("Restablecer contraseña")
