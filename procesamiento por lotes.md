Elegir la **Opción 2** es una decisión fantástica desde el punto de vista pedagógico. Obliga a los alumnos a entender el ciclo de vida de los datos en Spark: cómo se distribuye el trabajo en los nodos (*transformaciones*) y cómo regresa el resultado compilado al nodo central (*acciones*).

Para evitar el temido error de falta de memoria (`OutOfMemoryError`) en tu portátil Master al procesar los 32 GB, la estrategia consiste en **trocear el DataFrame original en lotes (chunks)** antes de llamar a la acción `.collect()`.

En Spark no existe un concepto nativo de "paginación" como en las bases de datos SQL tradicionales, pero podemos emularlo de forma muy eficiente de dos maneras diferentes para tu clase.

---

### Enfoque 1: El truco del identificador único (`monotonically_increasing_id`)

Este es el método más robusto en Spark. Le asignamos un número secuencial a cada ruta de archivo JSON detectada y luego procesamos los datos filtrando por rangos de ese número (por ejemplo, de 100 en 100 imágenes).

Aquí tienes cómo quedaría la estructura de tu **segunda celda** (el flujo de control de Spark):

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import json
import os

spark = SparkSession.builder \
    .appName("BacteriaDatasetProcessing") \
    .getOrCreate()

dataset_path = "/content/AGAR_demo_extracted/AGAR_representative/"

# 1. Leer todas las rutas de los archivos JSON
json_files_df = spark.read.format("binaryFile") \
    .option("pathGlobFilter", "*.json") \
    .option("recursiveFileLookup", "true") \
    .load(dataset_path) \
    .select("path")

# 2. Añadir una columna con un ID único para poder trocear el dataset
json_files_df = json_files_df.withColumn("row_id", monotonically_increasing_id())

total_archivos = json_files_df.count()
print(f"Total de archivos JSON detectados: {total_archivos}")

# 3. Definir el tamaño del lote (ajustar según la RAM de tu Master)
TAMANO_LOTE = 100  

# 4. Bucle para procesar por lotes
for inicio in range(0, total_archivos, TAMANO_LOTE):
    fin = inicio + TAMANO_LOTE
    print(f"🚀 Procesando lote: del archivo {inicio} al {fin}...")
    
    # Filtrar el DataFrame para coger solo el lote actual
    lote_df = json_files_df.filter((json_files_df["row_id"] >= inicio) & (json_files_df["row_id"] < fin))
    
    # Enviar el lote a los Workers y recoger los resultados del lote en memoria
    resultados_lote = lote_df.rdd.map(process_image_and_json).collect()
    
    # El Master procesa y escribe en su disco duro local ESTE lote concreto
    for item in resultados_lote:
        # Si el worker devolvió un error string, lo imprimes y saltas
        if isinstance(item, str) and item.startswith("Error"):
            print(item)
            continue
            
        filename = item["filename"]
        
        # Guardar el JSON corregido en el Master
        seg_json_dir = "/content/output/segmentation/labels"
        os.makedirs(seg_json_dir, exist_ok=True)
        with open(os.path.join(seg_json_dir, f"{filename}.json"), "w") as f:
            json.dump(item["new_json_data"], f, indent=4)
            
        # Guardar la imagen reescalada en el Master
        # (Nota: tendrías que reconvertir los bytes recibidos a formato imagen o guardarlos directamente)
        # ... lógica de guardado de imágenes ...

    print(f"✅ Lote {inicio}-{fin} guardado correctamente en el Master.")

spark.stop()

```

---

### Enfoque 2: Troceado nativo de RDDs con `randomSplit`

Si no quieres meter lógica de IDs numéricos, Spark tiene una función llamada `.randomSplit()` que divide un DataFrame o RDD en fragmentos basados en pesos porcentuales.

Por ejemplo, si tienes unas 2000 imágenes y quieres hacer 20 lotes del 5% cada uno:

```python
# Crear una lista de pesos: [0.05, 0.05, 0.05, ... hasta sumar 1.0]
pesos = [0.05] * 20 

# Spark divide el DataFrame de forma aleatoria y equilibrada en 20 sub-DataFrames
lotes = json_files_df.randomSplit(pesos, seed=42)

for i, lote_df in enumerate(lotes):
    print(f"Procesando lote aleatorio {i+1} de {len(lotes)}...")
    resultados = lote_df.rdd.map(process_image_and_json).collect()
    
    # ... Tu lógica de guardado en el disco del Master ...

```

* **Ventaja:** El código es extremadamente limpio.
* **Inconveniente:** `randomSplit` requiere un pequeño esfuerzo extra de computación por parte de Spark para barajar los datos proporcionalmente, pero para vuestro caso es totalmente despreciable.

---

### ⚠️ Ajuste clave en la función del Worker (`process_image_and_json`)

Para que la **Opción 2** funcione, recuerda modificar los `return` del código que probaste en Colab. Ya **no debes guardar nada dentro de la función**. En su lugar, debes pasar los datos binarios de las imágenes a través de la red de vuelta al Máster.

Para extraer los bytes de la imagen reescalada y de los recortes (*crops*) en memoria sin escribir en disco, puedes usar `io.BytesIO`:

```python
# --- Dentro del bucle de la función del worker ---

# Para la imagen reescalada:
img_ram = io.BytesIO()
img_resized.save(img_ram, format="JPEG")
bytes_img_reescalada = img_ram.getvalue()

# Al final de la función, en vez de un string, devuelves el diccionario:
return {
    "filename": filename_base,
    "new_json_data": data,
    "bytes_resized": bytes_img_reescalada
    # Si haces recortes (crops), puedes meterlos en una lista de bytes aquí dentro
}

```

### 👨‍🏫 Valor educativo para la clase:

Este enfoque por lotes te permite dar una lección magistral sobre **cuellos de botella en sistemas distribuidos**.

Les puedes hacer ver a los alumnos cómo la CPU de sus ordenadores (Workers) se pone al 100% durante el procesamiento del lote, y luego se queda a la espera (al 0%) mientras tu portátil Master se satura escribiendo los archivos recibidos en el disco duro local. Es una simulación perfecta de la arquitectura industrial de procesamiento de datos.