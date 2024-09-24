import os

# Definir la estructura de carpetas y archivos
structure = {
    "my_app": {
        "main.py": """# Archivo principal que inicia la aplicación
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
""",
        "gui": {
            "__init__.py": "# Hace que la carpeta sea un paquete Python\n",
            "main_window.py": """# Clase que define la ventana principal de la GUI
class MainWindow:
    def __init__(self):
        pass  # Inicializar ventana principal
""",
            "filters.py": """# Clase que maneja los filtros para la base de datos
class Filters:
    def apply_filters(self, data):
        pass  # Aplicar filtros a los datos
""",
            "reports.py": """# Clase que genera reportes en Excel
class Reports:
    def generate_report(self, data):
        pass  # Generar reporte en Excel
""",
        },
        "database": {
            "__init__.py": "",
            "db_connection.py": """# Clase que maneja la conexión a la base de datos
class DBConnection:
    def connect(self):
        pass  # Conectar a la base de datos
""",
            "queries.py": """# Archivo que contiene las consultas SQL
class Queries:
    def get_data(self):
        pass  # Obtener datos de la base de datos
""",
        },
        "reports": {
            "__init__.py": "",
            "report_template.xlsx": ""  # Este archivo se puede crear manualmente más tarde
        },
        "static": {
            "logo.png": "",  # Este archivo se puede agregar manualmente más tarde
            "styles.css": """body {
    font-family: Arial, sans-serif;
}

.container {
    width: 50%;
    margin: auto;
    padding: 20px;
}

h1 {
    text-align: center;
}

form {
    display: flex;
    flex-direction: column;
}

label {
    margin-bottom: 5px;
}

input {
    margin-bottom: 10px;
}
"""  # Estilos CSS para la GUI
        },
        "templates": {  # Carpeta para las plantillas HTML
            "index.html": """<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="{{ url_for('static', filename='styles.css') }}">
    <title>Filtrar Información</title>
</head>
<body>
    <div class="container">
        <h1>Filtrar Información</h1>
        <form action="/filter" method="POST">
            <label for="age_limit">Edad mínima:</label>
            <input type="number" id="age_limit" name="age_limit" required>
            <button type="submit">Filtrar</button>
        </form>
    </div>
</body>
</html>
""",
        },
        "requirements.txt": "Flask\nopenpyxl\npandas\n"  # Dependencias del proyecto
    }
}

def create_structure(base_path, structure):
    for name, content in structure.items():
        path = os.path.join(base_path, name)
        if isinstance(content, dict):
            # Crear directorio
            os.makedirs(path, exist_ok=True)
            # Recursivamente crear contenido dentro del directorio
            create_structure(path, content)
        else:
            # Crear archivo y escribir contenido si existe
            with open(path, 'w') as file:
                file.write(content)

# Crear la estructura de directorios y archivos
create_structure(".", structure)

print("Estructura de archivos y carpetas creada con éxito.")