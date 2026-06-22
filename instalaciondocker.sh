#!/bin/bash
# Script para instalar Docker Engine en Ubuntu 24.04 (repositorio oficial)

set -e  # Detener el script si algún comando falla

echo "===== Iniciando instalación de Docker en Ubuntu 24.04 ====="

# 1. Actualizar e instalar dependencias básicas
echo "[1/7] Actualizando sistema e instalando dependencias..."
sudo apt update -y
sudo apt install -y ca-certificates curl

# 2. Eliminar paquetes conflictivos previos (si existen)
echo "[2/7] Eliminando versiones anteriores de Docker (si existen)..."
sudo apt remove -y docker.io docker-compose docker-compose-v2 docker-doc podman-docker containerd runc || true

# 3. Configurar el repositorio oficial de Docker
echo "[3/7] Agregando clave GPG y repositorio de Docker..."
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Crear el archivo de fuente en formato DEB822 (recomendado para Ubuntu 24.04)
echo "Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc" | sudo tee /etc/apt/sources.list.d/docker.sources > /dev/null

# 4. Actualizar índice de paquetes con el nuevo repositorio
echo "[4/7] Actualizando índice de paquetes..."
sudo apt update -y

# 5. Instalar Docker Engine y sus componentes
echo "[5/7] Instalando Docker Engine, CLI, containerd y plugins..."
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 6. Iniciar y habilitar el servicio Docker
echo "[6/7] Iniciando y habilitando el servicio Docker..."
sudo systemctl start docker
sudo systemctl enable docker

# 7. Verificar instalación (prueba hello-world)
echo "[7/7] Verificando instalación con 'hello-world'..."
sudo docker run --rm hello-world

# 8. (Opcional) Agregar usuario actual al grupo docker para evitar sudo
echo ""
echo "===== Instalación completada con éxito ====="
echo ""
echo "Para ejecutar docker sin 'sudo', agrega tu usuario al grupo docker con:"
echo "  sudo usermod -aG docker $USER"
echo "IMPORTANTE: Cierra sesión y vuelve a entrar para que el cambio tenga efecto."
echo ""
echo "Versión instalada:"
sudo docker --version
sudo docker compose version

echo ""
echo "Ahora puedes ejecutar tu comando de Spark Worker:"
echo "  docker pull bitnamilegacy/spark:4.0.0-debian-12-r20"
echo "Si estás en un Worker:"
echo "docker run -d --name spark-worker --net host bitnamilegacy/spark:4.0.0-debian-12-r20 spark-class org.apache.spark.deploy.worker.Worker spark://IP_DE_TU_MASTER:7077"
echo "Si tienes la suerte de ser el Master:"
echo "docker run -d --name spark-master --net host bitnamilegacy/spark:4.0.0-debian-12-r20 spark-class org.apache.spark.deploy.master.Master"
