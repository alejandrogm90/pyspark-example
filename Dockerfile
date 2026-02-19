# Usar una imagen base de Ubuntu
FROM ubuntu:20.04

# Instalar dependencias necesarias
RUN apt-get update
# Configurar tzdata sin interacción
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata && \
    ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata
RUN apt-get install -y \
    python3 \
    python3-pip \
    openjdk-17-jdk \
    python3-setuptools

# Configurar variables de entorno
ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Crear un directorio de trabajo
WORKDIR /app

# Copiar archivos de la aplicación
COPY app/ ./

# Instalar dependencias
RUN pip3 install --no-cache-dir -r requirements.txt
