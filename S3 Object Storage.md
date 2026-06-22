# Sistema de Archivos Distribuido (HDFS)

En un entorno profesional, este problema de "las rutas y los usuarios de los alumnos" se resuelve abstrayendo el almacenamiento mediante un **Sistema de Archivos Distribuido (HDFS)**, un **Object Storage (como AWS S3, MinIO o Google Cloud Storage)**, o mediante una configuración de infraestructura idéntica en los nodos.

La solución estándar de la industria es utilizar un **Object Storage S3 Local (con MinIO)** o un **Punto de Montaje Compartido Universal (NFS)**.

---

### Opción A: El estándar Cloud / Enterprise (MinIO / S3)

En lugar de que cada portátil lea de su propio disco local, se levanta un servidor de almacenamiento común (Object Storage) usando **MinIO** (un clon de AWS S3 de código abierto que se levanta en un segundo con Docker).

1. El dataset se sube una sola vez al MinIO central.
2. Spark lee directamente del MinIO usando el protocolo universal `s3a://`.

**Tu código de Spark se vuelve mucho más limpio:**

```python
# El código pasa a ser independiente del sistema operativo y del usuario de tus alumnos
dataset_path = "s3a://bacteria-dataset/AGAR_representative/"

# Spark lee de forma nativa y óptima desde el almacenamiento centralizado
json_files_df = spark.read.format("binaryFile") \
    .option("pathGlobFilter", "*.json") \
    .option("recursiveFileLookup", "true") \
    .load(dataset_path)

```

**Ventajas:**

* Tus alumnos solo tienen que configurar dos variables de entorno en su Spark (`spark.hadoop.fs.s3a.access.key`, `spark.hadoop.fs.s3a.secret.key` y el nombre del bucket `dataset_path`, etc.).
* El código no cambia jamás, se ejecute en el portátil de un alumno, en tu servidor local o si el día de mañana te llevas la práctica a AWS EMR o Databricks.
* No hay problemas de rutas locales, ni de nombres de usuario, ni de `Permission denied`.

---

### Opción B: El estándar de Red Local (NFS / Shared Volume)

Si no quieres añadir la capa de almacenamiento de objetos, la alternativa profesional en servidores locales es utilizar un **NFS (Network File System)**.

Se configura una carpeta compartida en tu red local. En los ordenadores de los alumnos, esa carpeta de red se monta **exactamente en la misma ruta absoluta del sistema** (por ejemplo, `/mnt/shared_dataset`).

Al levantar Docker, tanto tú como tus alumnos mapeáis el volumen apuntando a esa ruta global:

```bash
docker run -v /mnt/shared_dataset:/mnt/shared_dataset ...

```

De esta forma, la ruta `file:///mnt/shared_dataset/...` es unívoca, real e idéntica en el Master, en el Worker de Juan, en el de Andrés y en el tuyo. Spark puede usar su lector nativo de Java a máxima velocidad sin errores.

---

### Desarrollo de la Opción A

Si quieres implementar la **Opción A (MinIO)**, que es la que les dará a tus alumnos la experiencia más cercana a trabajar en una empresa con entornos Cloud:

1. Levantas MinIO en tu máquina principal (Master).
2. Configuras la sesión de Spark para incluir los jars de Hadoop-AWS:
```python
spark = SparkSession.builder \
    .appName("BacteriaDataset") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.0.20:9000") \
    ...

```
3. Tu función `process_image_and_json` recibirá los bytes del archivo directamente desde Spark de forma distribuida, procesará usando Pillow en memoria y guardará el resultado de vuelta en el bucket S3 con un simple `boto3` o escribiendo el DataFrame.


#### Levantar MinIO en tu maquina

Como profesor, tú actuarás como el servidor de almacenamiento central. Solo necesitas levantar MinIO en tu portátil principal mediante Docker.

Crea una carpeta local para que los datos sean persistentes y arranca el contenedor:

```bash
# Crear directorio físico para el almacenamiento de datos
mkdir -p $HOME/minio_data

# Lanzar MinIO con la API de S3 (puerto 9000) y la interfaz Web (puerto 9001)
docker run -d \
  --name minio-server \
  -p 9000:9000 \
  -p 9001:9001 \
  -v $HOME/minio_data:/data \
  -e "MINIO_ROOT_USER=admin_clase" \
  -e "MINIO_ROOT_PASSWORD=PasswordClase2026" \
  minio/minio server /data --console-address ":9001"

```

### Paso 2: Subir el dataset al Bucket

1. Abre tu navegador web e ingresa a la consola de administración: `http://localhost:9001`.
2. Inicia sesión con las credenciales que configuraste (`admin_clase` / `PasswordClase2026`).
3. Ve a la sección **Buckets**, haz clic en **Create Bucket** y asígnale el nombre `bacteria-dataset`.
4. Sube la carpeta `AGAR_representative` estructurada directamente al bucket (puedes arrastrarla desde la interfaz web o usar el cliente de consola `mc` de MinIO si prefieres automatizarlo).

---

### Paso 3: Configurar el entorno de tus Alumnos (Workers)

Tus alumnos ya no necesitan clonar gigabytes de imágenes en sus discos locales. Su infraestructura se vuelve extremadamente ligera. Solo necesitan levantar el Worker apuntando a tu Master de Spark (por ejemplo, en la IP `192.168.0.20`), de manera idéntica a como lo tenías:

```bash
docker run -d --name spark-worker \
  --net host \
  bitnamilegacy/spark:4.0.0-debian-12-r20 \
  spark-class org.apache.spark.deploy.worker.Worker spark://192.168.0.20:7077
```

---

### Paso 4: El Script de Spark (En tu Jupyter Notebook)

Ahora, el truco de magia: para que Spark pueda comunicarse con MinIO mediante el protocolo `s3a://`, necesita los conectores nativos de AWS para Hadoop. No tienes que instalarlos manualmente; Spark los descarga e instala de forma transparente si los declaras como dependencias en el constructor mediante `.config("spark.jars.packages", ...)`.

Este es el esqueleto del código limpio, unificado y profesional para lanzar en clase:

```python
import os
from pyspark.sql import SparkSession

# IP de tu ordenador principal (donde corren el Master de Spark y MinIO)
IP_CENTRAL = "192.168.0.20"

# Inicializar sesión de Spark con los conectores S3A nativos
spark = SparkSession.builder \
    .master(f"spark://{IP_CENTRAL}:7077") \
    .config("spark.driver.host", IP_CENTRAL) \
    .appName("BacteriaDatasetProcessing_S3") \
    \
    # 🌟 DESCARGA AUTOMÁTICA DE JARS NATIVOS (Ajusta según tu versión de Spark/Hadoop)
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    \
    # 🌟 CONFIGURACIÓN DEL PROTOCOLO S3A APUNTANDO A TU MINIO
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{IP_CENTRAL}:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin_clase") \
    .config("spark.hadoop.fs.s3a.secret.key", "PasswordClase2026") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("¡Conectado con éxito al Clúster y al Almacenamiento S3 de MinIO!")

# 🌟 RUTA UNIVERSAL CLOUD
# No importa el usuario, ni el sistema operativo, ni el nodo. La ruta es unívoca.
dataset_path = "s3a://bacteria-dataset/AGAR_representative/"

# Lector nativo optimizado de Spark
json_files_df = spark.read.format("binaryFile") \
    .option("pathGlobFilter", "*.json") \
    .option("recursiveFileLookup", "true") \
    .load(dataset_path) \
    .select("path")

print(f"Número de archivos JSON detectados de forma nativa en MinIO: {json_files_df.count()}")

```

---

### Paso 5: Cómo consume los datos la función en los Workers

En este punto, cuando ejecutes `.rdd.map(process_image_and_json)`, cada fila del DataFrame enviará a los hilos de tus alumnos rutas estructuradas como `s3a://bacteria-dataset/AGAR_representative/lower-resolution/14512.json`.

Para mantener la función de procesamiento (`process_image_and_json`) limpia y profesional, tus alumnos ya no usarán el método `with open()` nativo de Python para leer archivos locales de disco, sino una librería estándar de la industria Cloud como **`boto3`** o el propio sistema de archivos de Spark para descargar o guardar los bytes directamente en memoria.

Si quieres, podemos reestructurar esa función de Pillow usando `boto3` para que consuma y guarde los *crops* directamente en un bucket de salida (`s3a://bacteria-dataset/output/`) de forma 100% nativa en la nube.