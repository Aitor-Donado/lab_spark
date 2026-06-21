## Reparto de archivos automático

**Es 100% automático, pero con una condición crítica sobre los datos.**

Spark se encarga de todo. Al ejecutar `spark.read.format("binaryFile").load(dataset_path)`, el Master escanea el directorio, hace una lista de los archivos JSON encontrados y divide esa lista en fragmentos (_partitions_). Luego, le dice al Worker 1: _"Tú procesas del archivo 1 al 10"_, y al Worker 2: _"Tú del 11 al 20"_. Tú no tienes que programar nada del reparto.

**La condición crítica:** Los datos **tienen que ser accesibles por los Workers en la misma ruta**.

Como estás usando Docker (`bitnamilegacy/spark`), cuando arranques los contenedores en los portátiles viejos (Workers), tienes que montar la carpeta donde están las imágenes usando volúmenes de Docker (`-v`).

Si en tu Master las imágenes están en `/home/laptop/lab_spark/input`, en los ordenadores de los alumnos (o tus portátiles viejos) las imágenes deben estar **exactamente** en `/home/laptop/lab_spark/input`.

## 🏁 Pasos para que tu prueba en casa sea exitosa

Para replicar lo de Colab en tu red local con los tres portátiles, sigue este orden:

### Paso 1: Preparar la carpeta compartida en red (O replicar datos)

Para tu prueba en casa, lo más rápido es que **copies la carpeta de la demo exactamente en la misma ruta en los 3 portátiles** (ej: crear una carpeta llamada `/home/laptop/lab_spark/input` en los tres sistemas operativos).

### Paso 2: Lanzar los contenedores Docker vinculando esa carpeta

Al levantar los Workers en los portátiles viejos, debes mapear esa carpeta del sistema operativo hacia el contenedor con el parámetro `-v`:

Bash

```
docker run -d --name spark-worker \
  --net host \
  -v /home/laptop/lab_spark/input:/home/laptop/lab_spark/input \
  bitnamilegacy/spark:4.0.0-debian-12-r20 \
  spark-class org.apache.spark.deploy.worker.Worker spark://IP_DE_TU_MASTER:7077
```

_Nota: Cambia `/home/laptop/lab_spark/input` por la ruta real que decidas usar. Al usar `--net host`, el puerto 8081 del worker se abrirá en la red local._

> 💡 **¿Qué significa `-v /home/laptop/lab_spark/input:/home/laptop/lab_spark/input`?**
> 
> Lo que está a la **izquierda** de los dos puntos `:` es la carpeta real en el disco duro de tu portátil viejo. Lo que está a la **derecha** es la ruta virtual dentro del contenedor Docker. Al ponerlas iguales, garantizamos que el script de Spark funcione sin importar si se ejecuta "dentro" o "fuera" de Docker.
### Paso 3: Modificar las rutas de salida en el Script

Asegúrate de que `output_dir` apunte a esa misma ruta mapeada (por ejemplo, `/home/laptop/lab_spark/output`), de modo que cuando un worker escriba el recorte de la bacteria, lo guarde en su propio disco local en una ruta válida.

```python
# Ruta de entrada (apuntando a la raíz del volumen)
dataset_path = "/home/laptop/lab_spark/input/AGAR_representative/"

# Ruta de salida (se creará automáticamente dentro del volumen)
output_dir = "/home/laptop/lab_spark/output"
```

Al terminar el proceso, verás cómo en el disco duro real de tus portátiles viejos (en `/home/laptop/lab_spark/output`) aparecen las carpetas de `crops` y `segmentation` con las imágenes ya procesadas por sus respectivas CPUs.

¿Ves como no era para tanto? Es la forma correcta de trabajar con contenedores y datos locales.

### Paso 4: Ejecutar el script desde el Master apuntando al Clúster

En tu portátil Master, ejecuta el script de Python asegurándote de que la sesión de Spark se conecte a tu clúster y no en modo local:

Python

```
spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("BacteriaDatasetProcessing") \
    .getOrCreate()
```

Lánzalo y revisa la interfaz web en `localhost:8080`. Verás cómo se crea una aplicación y cómo los dos workers ALIVE empiezan a procesar en paralelo las imágenes de las bacterias.