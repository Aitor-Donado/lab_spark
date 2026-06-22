# PySpark en Google Colab

---

## 1. ¿Qué es PySpark?

PySpark es la API de Python para Apache Spark, un motor de procesamiento distribuido diseñado para trabajar con grandes volúmenes de datos. Aunque en Colab no tendremos un clúster real, podemos ejecutar Spark en modo local (`local[*]`) para aprender su sintaxis y funcionalidades.

---

## 2. Instalación de PySpark en Colab

En Colab, basta con ejecutar el siguiente comando para instalar la librería:

```python
!pip install pyspark
```

---

## 3. Creación de la sesión y contexto de Spark

Para trabajar con Spark necesitamos crear una **SparkSession**. Esta sesión es la puerta de entrada a todas las funcionalidades de Spark (DataFrames, SQL, Streaming, etc.).

```python
from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("MiPrimerTutorial") \
    .getOrCreate()

# El SparkContext se encuentra dentro de la sesión
sc = spark.sparkContext

print("Sesión creada correctamente")
print(f"Versión de Spark: {sc.version}")
```

### Explicación de conceptos

- **SparkSession**: Es el punto de entrada unificado. Reemplaza a las antiguas `SQLContext` y `HiveContext`. Permite acceder a DataFrames, ejecutar SQL, leer datos, etc.
- **SparkContext**: Es el corazón de la conexión con el clúster. Maneja la configuración, la asignación de recursos y la ejecución de tareas. Se puede obtener a través de `spark.sparkContext`.
- **Builder**: Es un patrón de diseño que nos permite configurar la sesión paso a paso. Con `builder` podemos:
  - Establecer el nombre de la aplicación (`.appName("nombre")`).
  - Definir el modo de ejecución (`.master("local[*]")`). Si no se especifica, por defecto usa `local[*]` en Colab.
  - Añadir configuraciones adicionales (`.config("clave", "valor")`).
  - Habilitar soporte para Hive (`.enableHiveSupport()`), si se necesita.

### Parámetros comunes en el Builder (aunque no montemos un clúster)

| Parámetro | Descripción | Ejemplo |
|-----------|-------------|---------|
| `.master("local[*]")` | Define el modo de ejecución. `local[*]` usa todos los núcleos disponibles en la máquina local. | `.master("spark://master:7077")` para un clúster standalone. |
| `.appName("nombre")` | Asigna un nombre identificativo a la aplicación. | `.appName("MiApp")` |
| `.config("spark.some.config.option", "value")` | Permite establecer cualquier configuración de Spark. | `.config("spark.sql.shuffle.partitions", "10")` |
| `.enableHiveSupport()` | Habilita la integración con Hive (necesita dependencias). | `.enableHiveSupport()` |

**Ejemplo completo con configuración personalizada:**

```python
spark = SparkSession.builder \
    .appName("ConfiguracionEjemplo") \
    .master("local[4]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

---

## 4. Carga de datos desde un CSV

Usaremos el conjunto de datos **Iris** (clásico en machine learning) alojado en un repositorio público.

```python
# URL del CSV
url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"

# Leer el CSV con opciones: cabecera y esquema inferido
df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv(url)

print("Datos cargados correctamente")
```

---

## 5. Operaciones básicas con DataFrames

A continuación, realizaremos las operaciones más comunes sobre el DataFrame `df`.

### 5.1. Mostrar un número determinado de filas

```python
# Mostrar las primeras 5 filas (por defecto son 20)
df.show(5)
```

### 5.2. Ver el esquema (printSchema)

```python
df.printSchema()
```

### 5.3. Ver las columnas y sus tipos, y descripción estadística

```python
# Lista de columnas y sus tipos
print("Columnas y tipos:")
for col_name, col_type in df.dtypes:
    print(f"{col_name}: {col_type}")

# Descripción estadística de las columnas numéricas
df.describe().show()
```

### 5.4. Seleccionar una o varias columnas

```python
# Seleccionar una columna
df.select("sepal_length").show(5)

# Seleccionar varias columnas
df.select("sepal_length", "petal_length", "species").show(5)
```

### 5.5. Agregar nuevas columnas (con `withColumn`)

Vamos a crear una columna `sepal_area` que sea el producto de `sepal_length` por `sepal_width`.

```python
from pyspark.sql.functions import col

df_con_area = df.withColumn("sepal_area", col("sepal_length") * col("sepal_width"))
df_con_area.select("sepal_length", "sepal_width", "sepal_area").show(5)
```

### 5.6. Agrupar por valores de una o varias columnas

Agrupamos por `species` y calculamos el promedio de `petal_length`.

```python
from pyspark.sql.functions import avg

df.groupBy("species").agg(avg("petal_length").alias("avg_petal_length")).show()
```

También podemos agrupar por varias columnas (aunque en Iris solo hay una categoría).

### 5.7. Filtrar filas por el valor de una o varias columnas

Filtrar las filas donde `sepal_length` sea mayor a 5.0 y `species` sea "setosa".

```python
df_filtrado = df.filter((col("sepal_length") > 5.0) & (col("species") == "setosa"))
df_filtrado.show(5)
```

### 5.8. Eliminar columnas (con `drop`)

Eliminamos la columna `sepal_area` (que agregamos antes).

```python
# Si tenemos el DataFrame con la columna añadida
df_sin_area = df_con_area.drop("sepal_area")
df_sin_area.columns  # Verificamos que ya no está
```

### 5.9. Ordenar filas (con `orderBy` o `sort`)

Ordenamos por `sepal_length` de forma descendente.

```python
df.orderBy(col("sepal_length").desc()).show(5)
```

### 5.10. Merge (join) entre dos DataFrames

Para hacer un join, necesitamos un segundo DataFrame. Crearemos uno con las especies y una descripción adicional.

```python
# Crear un DataFrame de especies con información extra
data_especies = [("setosa", "Iris setosa"), 
                 ("versicolor", "Iris versicolor"), 
                 ("virginica", "Iris virginica")]
columnas_especies = ["species", "nombre_comun"]
df_especies = spark.createDataFrame(data_especies, columnas_especies)

# Hacer un inner join con el DataFrame original
df_join = df.join(df_especies, on="species", how="inner")
df_join.select("species", "nombre_comun", "sepal_length", "petal_length").show(5)
```

---
## Conclusión

Has aprendido a instalar PySpark en Colab, crear una sesión, entender los conceptos de SparkSession y SparkContext, y realizar las operaciones más habituales sobre DataFrames: mostrar, esquema, estadísticas, selección, creación de columnas, agrupación, filtrado, eliminación, ordenación y joins.

Estos fundamentos te servirán para empezar a trabajar con conjuntos de datos más grandes y aprovechar el poder de Spark en entornos distribuidos.