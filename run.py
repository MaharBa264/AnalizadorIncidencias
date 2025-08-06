from analizador import create_app

app = create_app()

if __name__ == '__main__':
    # El host '0.0.0.0' permite que sea accesible desde la red
    app.run(host='0.0.0.0', port=5000)