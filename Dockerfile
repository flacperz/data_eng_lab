# Usamos una imagen que ya tiene Java 17 instalado (basada en Ubuntu Jammy)
FROM eclipse-temurin:17-jre-jammy

# Instalamos Python y herramientas básicas
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Crear un enlace para usar 'python' en lugar de 'python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Instalación de dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del proyecto
COPY . /app

# CMD ["python", "src/main.py"]
CMD ["bash"]