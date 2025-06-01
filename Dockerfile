
FROM python:3.10-slim

# Instala Java (necesario para Spark)
RUN apt-get update && apt-get install -y openjdk-11-jre && rm -rf /var/lib/apt/lists/*

# Variables de entorno para Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=python3

# Instala dependencias de Python
#COPY requirements.txt .
FROM ubuntu:20.04

# Instala dependencias base
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    openjdk-11-jre \
    python3 \
    python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Copia la app y las dependencias
COPY . /app
WORKDIR /app
#todo : Reemplazar con requirements.txt si existe
RUN pip install kagglehub
RUN pip install pyspark
RUN pip install numpy
# Instala cualquier otra dependencia necesaria
# Ejecuta tu script por defecto
CMD ["python", "main.py"]
