# PySpark para modificar un dataset de 32GB

### 📅 Planificación de las 6 horas de clase:

1. **Bloque 1 (1.5 horas): Red y Cluster.** Explicación teórica de la computación distribuida de archivos binarios. Configuración de la red local, montajes NFS compartidos o distribución de datos, y levantamiento del clúster de Spark (comprobación en la interfaz web `8080`).
2. **Bloque 2 (2 horas): Desarrollo del Pipeline.** Programación del script. Es importante probar de forma local en una máquina con la "demo pequeña" para validar que las funciones de Pillow calculan bien los recortes y las nuevas coordenadas JSON.
3. **Bloque 3 (2 horas): El Gran Lanzamiento.** Subir el script al Spark Master de la clase y ver cómo procesa los 32 GB en paralelo. Monitorear los gráficos de consumo de CPU de la interfaz web de Spark.
4. **Última media hora:** Verificación del dataset resultante y conclusiones.

## El problema

Eguzkiñe tiene un dataset de 32GB de imágenes para su proyecto final. 

Son imágenes 4K de muestras de colonias de bacterias. 

Me ha pasado una demo con una muestra de lo que contiene su dataset:

```
.
└── AGAR_representative
    ├── higher-resolution
    │   ├── bright
    │   │   ├── 1399.jpg
    │   │   ├── 1399.json
    │   │   ├── 349.jpg
    │   │   ├── 349.json
    │   │   ├── 356.jpg
    │   │   ├── 356.json
    │   │   ├── 510.jpg
    │   │   ├── 510.json
    │   │   ├── 518.jpg
    │   │   ├── 518.json
    │   │   ├── 525.jpg
    │   │   ├── 525.json
    │   │   ├── 734.jpg
    │   │   ├── 734.json
    │   │   ├── 735.jpg
    │   │   ├── 735.json
    │   │   ├── 736.jpg
    │   │   ├── 736.json
    │   │   ├── 971.jpg
    │   │   └── 971.json
    │   ├── dark
    │   │   ├── 4826.jpg
    │   │   ├── 4826.json
    │   │   ├── 4904.jpg
    │   │   ├── 4904.json
    │   │   ├── 5206.jpg
    │   │   ├── 5206.json
    │   │   ├── 5207.jpg
    │   │   ├── 5207.json
    │   │   ├── 5212.jpg
    │   │   ├── 5212.json
    │   │   ├── 5270.jpg
    │   │   ├── 5270.json
    │   │   ├── 5271.jpg
    │   │   ├── 5271.json
    │   │   ├── 5308.jpg
    │   │   ├── 5308.json
    │   │   ├── 5312.jpg
    │   │   ├── 5312.json
    │   │   ├── 8386.jpg
    │   │   └── 8386.json
    │   └── vague
    │       ├── ==11761.jpg==
    │       ├── ==11761.json==
    │       ├── 11764.jpg
    │       ├── 11764.json
    │       ├── 11773.jpg
    │       ├── 11773.json
    │       ├── 11884.jpg
    │       ├── 11884.json
    │       ├── 11890.jpg
    │       ├── 11890.json
    │       ├── 11924.jpg
    │       ├── 11924.json
    │       ├── 11955.jpg
    │       ├── 11955.json
    │       ├── 12028.jpg
    │       ├── 12028.json
    │       ├── 12031.jpg
    │       ├── 12031.json
    │       ├── 12033.jpg
    │       └── 12033.json
    └── lower-resolution
        ├── 13895.jpg
        ├── 13895.json
        ├── 13938.jpg
        ├── 13938.json
        ├── 14130.jpg
        ├── 14130.json
        ├── 14380.jpg
        ├── 14380.json
        ├── 14410.jpg
        ├── 14410.json
        ├── 14512.jpg
        ├── 14512.json
        ├── 14581.jpg
        ├── 14581.json
        ├── 14618.jpg
        ├── 14618.json
        ├── 14627.jpg
        ├── 14627.json
        ├── 14684.jpg
        └── 14684.json
```

Esta es una imagen de las del ejamplo. La `11761.jpg` de la carpeta `vague`.

![[Pasted image 20260621085448.png]]

Cada imagen va acompañada de un json que segmenta la ubicación de la colonia en la imagen y la clase a la que pertenece dicha colonia.
```
{
    "background": "vague",
    "classes": [
        "P.aeruginosa",
        "S.aureus"
    ],
    "colonies_number": 54,
    "labels": [
        {
            "class": "S.aureus",
            "height": 39,
            "id": 1,
            "width": 39,
            "x": 706,
            "y": 2112
        },
        ...Más labels aquí...
        ],
    "sample_id": 11761
}
```

![[11761_1.jpeg|273]]


Su proyecto consiste en automatizar la segmentación de imágenes y luego reconocer las bacterias. Hemos pensado crear dos datasets. Uno para cada tarea.
- En el dataset de segmentación es posible (incluso, recomendable) reducir la resolución de las imágenes hasta el tamaño típico de imagen que vaya a consumir la red convolucional (esto reducirá considerablemente el tamaño del dataset), 
- Posteriormente habrá que corregir los valores x, y, width y height de los json para que sean compatibles con el nuevo tamaño de imagen para el entrenamiento de la red neuronal de segmentación.
- En el otro dataset, realizaremos la tarea de clasificación (identificación) de las colonias con los segmentos de las imágenes en los cuales aparecen esas colonias, (serán recortes más pequeños y en total ocuparán menos porque no aparecen las zonas vacías) Tienen que ir distribuidos en carpetas según la clase, como vimos en el ejercicio de identificar las setas a partir de fotografías.
- Para que los recortes conserven la calidad de imagen de la foto original, **tenemos que hacer la extracción de los recortes a partir de la imagen que se ha cargado antes de la reducción de la resolución**.

---

## 🛠️ El Plan de Ataque (Estrategia Rápida)

Para que funcione, necesitas que una máquina actúe como **Master** (probablemente la tuya) y las de tus alumnos como **Workers**.

### Requisitos previos en la red

1. **IPs accesibles:** Todas las máquinas deben poder hacerse `ping` entre sí.
2. **Puertos abiertos:** Asegúrate de que el firewall de Ubuntu (`ufw`) no esté bloqueando los puertos de Spark. Para la prueba, pueden desactivarlo temporalmente con `sudo ufw disable` o permitir los puertos específicos (`7077`, `8080`, `8081`).

---

## 🚀 Opción A: Despliegue Nativo
Si se quiere se puede instalar Spark, que es una aplicación escrita en java, estos son los comandos exactos que deben ejecutar.

### 1. En TODAS las máquinas (Master y Workers)
Tienen que instalar Java y descargar Spark. Abre la terminal y ejecuta:

```bash
# Actualizar e instalar Java (Spark 3.x requiere Java 8 o 11/17)
sudo apt update && sudo apt install -y default-jre

# Descargar Spark (ejemplo con Spark 3.5.1)
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvzf spark-3.5.1-bin-hadoop3.tgz
cd spark-3.5.1-bin-hadoop3
```

### 2. En tu máquina (Master)
Arranca el nodo central:
```bash
./sbin/start-master.sh
```

* **Verificación:** Abre en tu navegador `http://localhost:8080`. Ahí verás la URL de Spark Master (algo como `spark://IP_DE_TU_ORDENADOR:7077`). **Diles esa IP/URL distribuyéndola en la pizarra.**

### 3. En las máquinas de los alumnos (Workers)
Cada alumno solo tiene que ejecutar una línea apuntando a tu máquina:
```bash
./sbin/start-worker.sh spark://IP_DE_TU_ORDENADOR:7077
```

¡Listo! Si refrescas tu interfaz web (`http://localhost:8080`), verás cómo van apareciendo los cores y la RAM de tus alumnos sumándose al cluster en tiempo real. Es un momento "mágico" visualmente.

---

## 🐳 Opción B: Con Docker (La vía limpia y rápida)
Si tus alumnos ya tienen Docker instalado, te evitas configuraciones de Java.

1. **Tú (Master):**
```bash
docker run -d --name spark-master --net host bitnamilegacy/spark:4.0.0-debian-12-r20 spark-class org.apache.spark.deploy.master.Master
```

2. **Alumnos (Workers):**
```bash
docker run -d --name spark-worker \
  --net host \
  -v /home/alumno/lab_spark:/home/alumno/lab_spark \
  bitnamilegacy/spark:4.0.0-debian-12-r20 \
  spark-class org.apache.spark.deploy.worker.Worker spark://192.168.8.41:7077
```

Si no lo tienen instalado, aquí hay un script de bash para su instalación:

```bash
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
sudo usermod -aG docker $USER
echo "IMPORTANTE: Cierra sesión y vuelve a entrar para que el cambio tenga efecto."
echo ""
echo "Versión instalada:"
sudo docker --version
sudo docker compose version

echo ""
echo "  docker pull bitnamilegacy/spark:4.0.0-debian-12-r20"
echo "Si estás en un Worker:"
echo "docker run -d --name spark-worker --net host bitnamilegacy/spark:4.0.0-debian-12-r20 spark-class org.apache.spark.deploy.worker.Worker spark://IP_DE_TU_MASTER:7077"
echo "Si tienes la suerte de ser el Master:"
echo "docker run -d --name spark-master --net host bitnamilegacy/spark:4.0.0-debian-12-r20 spark-class org.apache.spark.deploy.master.Master"
```
Ahora podemos instalar un contenedor de Spark

> En mis pruebas ha sido necesario reiniciar la sesión después de ejecutar `sudo usermod -aG docker $USER` para que se tenga el permiso de ejecución de `docker` en la terminal sin usar `sudo`. Es mejor no usar `sudo` para ejecutar contenedores de Docker ya que éstos pertenecerían al usuario `root` y sería más difícil configurarlos.

```bash
docker pull bitnamilegacy/spark:4.0.0-debian-12-r20
```

Si estás en un Worker, se puede lanzar cuando sepamos la `IP_DE_TU_MASTER`:
```bash
docker run -d --name spark-worker --net host bitnamilegacy/spark:4.0.0-debian-12-r20 spark-class org.apache.spark.deploy.worker.Worker spark://IP_DE_TU_MASTER:7077
```

Si tienes la suerte de ser el Master:
```bash
docker run -d --name spark-master --net host bitnamilegacy/spark:4.0.0-debian-12-r20 spark-class org.apache.spark.deploy.master.Master
```

Desde el Master se pueden ver los nodos Worker y la memoria RAM que aportan en el navegador usando la dirección `localhost:8080`

---

### 🏛️ La Arquitectura del Reto: Procesamiento Binario en Spark

Cuando trabajamos con imágenes en Spark, no podemos tratarlas como un archivo de texto secuencial. El truco consiste en usar el formato `binaryFile`. Spark leerá los archivos de la red/disco local como un RDD o DataFrame de bytes, y repartirá las rutas de los archivos a los nodos de tus alumnos.

Cada ordenador de la clase recibirá un subconjunto de imágenes y sus JSON asociados, ejecutará los recortes y reescalados usando **Pillow (PIL)** u OpenCV, recalculará las cajas de la segmentación (*bounding boxes*) y guardará el resultado.

---

### 🛠️ Código Base para el Script de PySpark

Este script realiza las tres tareas que necesitas:

1. Reescalar la imagen original 4K a un tamaño menor (ej. `1024x1024`).
2. Adaptar las coordenadas del JSON proporcionalmente al nuevo tamaño.
3. Hacer recortes (*crops*) de las colonias y guardarlos en carpetas organizadas por su tipo de bacteria (`B.subtilis`, etc.).

Podéis preparar este archivo como `transform_dataset.py`:

```python
import os
import json
import io
from PIL import Image

# Dimensiones objetivo para el dataset de segmentación
TARGET_WIDTH = 1024
TARGET_HEIGHT = 1024
ORIGINAL_WIDTH = 2048  # Son imágenes cuadradas
ORIGINAL_HEIGHT = 2048

SCALE_X = TARGET_WIDTH / ORIGINAL_WIDTH
SCALE_Y = TARGET_HEIGHT / ORIGINAL_HEIGHT

# Ruta de salida
# output_dir = "/mnt/shared_dataset/output"
output_dir = "/content/output"
os.makedirs(output_dir, exist_ok=True)

def process_image_and_json(row):
    """
    Función que se ejecutará en los ordenadores de los alumnos (Workers).
    Recibe la ruta del JSON y busca su imagen emparejada.
    """
    json_path = row['path']

    # 🌟 CORRECCIÓN: Eliminar el protocolo 'file:' si Spark lo añade
    if json_path.startswith("file:"):
        json_path = json_path.replace("file:", "")
    # Reemplazar la extensión para obtener la imagen
    img_path = json_path.replace(".json", ".jpg")

    if not os.path.exists(img_path):
        return f"Error: Imagen no encontrada para {json_path}"

    try:
        # 1. Leer el JSON original
        with open(json_path, 'r') as f:
            data = json.load(f)

        # 2. Cargar la imagen original
        with open(img_path, 'rb') as f:
            img_bytes = f.read()
        img = Image.open(io.BytesIO(img_bytes))

        # --- TAREA 1 & 2: REESCALADO DE IMAGEN Y AJUSTE DE JSON ---
        img_resized = img.resize((TARGET_WIDTH, TARGET_HEIGHT))

        # Estructura para el nuevo JSON modificado
        new_labels = []
        for label in data.get("labels", []):
            # Recalcular las cajas de segmentación proporcionalmente
            new_x = int(label["x"] * SCALE_X)
            new_y = int(label["y"] * SCALE_Y)
            new_w = int(label["width"] * SCALE_X)
            new_h = int(label["height"] * SCALE_Y)

            new_labels.append({
                "id": label["id"],
                "class": label["class"],
                "x": new_x,
                "y": new_y,
                "width": new_w,
                "height": new_h
            })

            # --- TAREA 3: RECORTE (CROP) DE LA COLONIA ---
            # Coordenadas originales para el recorte de máxima calidad
            x, y, w, h = label["x"], label["y"], label["width"], label["height"]
            colony_crop = img.crop((x, y, x + w, y + h))

            # Definir ruta de salida del recorte por clase (Ej: /output/crops/B.subtilis/13895_1.jpg)
            crop_dir = f"{output_dir}/crops/{label['class']}"
            os.makedirs(crop_dir, exist_ok=True)
            crop_filename = f"{data['sample_id']}_{label['id']}.jpg"
            colony_crop.save(os.path.join(crop_dir, crop_filename))

        # Guardar imagen reescalada y nuevo JSON
        seg_img_dir = f"{output_dir}/segmentation/images"
        seg_json_dir = f"{output_dir}/segmentation/labels"
        os.makedirs(seg_img_dir, exist_ok=True)
        os.makedirs(seg_json_dir, exist_ok=True)

        filename_base = os.path.basename(json_path).replace(".json", "")
        img_resized.save(os.path.join(seg_img_dir, f"{filename_base}.jpg"))

        data["labels"] = new_labels
        with open(os.path.join(seg_json_dir, f"{filename_base}.json"), 'w') as f:
            json.dump(data, f, indent=4)

        return f"Procesado con éxito: {filename_base}"

    except Exception as e:
        return f"Error procesando {json_path}: {str(e)}"

```

y en el main:

```python

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BacteriaDatasetProcessing") \
    .getOrCreate()

# Buscamos todos los archivos .json del dataset
# (Asumiendo que el dataset está montado de forma compartida en la misma ruta para todos)
dataset_path = "/content/AGAR_demo_extracted/AGAR_representative/"

# Leemos los paths usando Spark
json_files_df = spark.read.format("binaryFile") \
    .option("pathGlobFilter", "*.json") \
    .option("recursiveFileLookup", "true") \
    .load(dataset_path) \
    .select("path")

# 🌟 DEBÚGUEO: Verifica cuántos archivos ha detectado Spark antes de procesar
print(f"Número de archivos JSON detectados por Spark: {json_files_df.count()}")

# Convertimos a RDD para procesar fila por fila de manera distribuida
results = json_files_df.rdd.map(process_image_and_json).collect()

# Imprimir resumen en el Driver
for res in results: # Muestra los primeros 10 resultados
    print(res)

spark.stop()

```

---
[[Ejecución con carpetas idénticas en los workers]]
### ⚠️ El verdadero desafío en clase: El Almacenamiento Compartido

Aquí, **los ordenadores de tus alumnos necesitan acceder a los 32 GB de imágenes.** Tienes dos opciones para resolver esto en las primeras horas de clase:

#### Opción 1: El enfoque Big Data Real (NFS / Carpeta Compartida)

Configura tu máquina (Master) como un servidor **NFS (Network File System)** en Ubuntu y haz que todos los alumnos lo monten en la misma ruta exacta (ej. `/mnt/shared_dataset`).

* **Ventaja:** El código corre tal cual. Spark leerá de tu disco a través de la red local.
* **Riesgo:** Si 15 o 20 alumnos saturan tu tarjeta de red leyendo archivos 4K simultáneamente, la red local puede convertirse en el cuello de botella. Sabiendo que estáis en un entorno controlado, aseguraos de conectaros por cable si es posible, o bajad el número de hilos de ejecución por alumno.

#### Opción 2: Distribución Local previa (El enfoque rápido)

Divide los 32 GB del dataset en "bloques" y dale un bloque diferente a cada alumno mediante un pendrive o una descarga local en sus discos duros.

* Si configuras PySpark para que apunte a `/home/alumno/dataset`, cada ordenador procesará sus archivos locales de forma distribuida si el script se lanza de manera local. Sin embargo, para hacerlo mediante un **único Spark Master distribuido**, es imperativo que las rutas de archivos sean idénticas o usar un sistema de archivos distribuido simple.



