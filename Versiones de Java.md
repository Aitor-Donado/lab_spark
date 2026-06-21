¡El clásico error de versiones de Java! Te acaba de saltar el archiconocido `UnsupportedClassVersionError`.

Lo que está pasando aquí es que al instalar `pyspark` en tu entorno virtual local (que por defecto se habrá bajado la última versión disponible, compatible con la versión 4.0 de Spark que estás testeando), la librería de Python empaqueta archivos Java binarios internos compilados para **Java 17** (que corresponde a la versión de clase 61.0).

Sin embargo, tu sistema operativo local tiene configurado por defecto **Java 8** (la versión de clase 52.0). Cuando Spark intenta arrancar, se encuentra con que tu Java local es "demasiado viejo" para entender el Spark moderno que se ha instalado en el entorno virtual.

Como estás en Ubuntu 24.04, solucionarlo es facilísimo y te llevará un minuto.

---

### 🛠️ La Solución: Instalar y activar Java 17 o Java 21

Ubuntu 24.04 maneja múltiples versiones de Java de forma nativa. Solo necesitas instalar la versión correcta y decirle al sistema (o a tu terminal) que la use.

#### Paso 1: Instalar Java 17 (o 21)

Abre tu terminal y ejecuta:

```bash
sudo apt update
sudo apt install -y openjdk-17-jre openjdk-17-jdk

```

#### Paso 2: Cambiar la versión por defecto del sistema

Para asegurarte de que tu Ubuntu use esta nueva versión, ejecuta:

```bash
sudo update-alternatives --config java

```

Te saldrá una lista en la terminal con las versiones de Java instaladas. **Selecciona el número que corresponda a Java 17** (ej. `/usr/lib/jvm/java-17-openjdk-amd64/...`) y pulsa Enter.

#### Paso 3: Verificar el cambio

En la misma terminal, comprueba que ya estás en la versión correcta:

```bash
java -version

```

Debería devolverte algo como `openjdk version "17.0.x" ...`.

---

### 💡 Un truco limpio (Por si no quieres cambiar el Java de todo tu sistema)

Si por temas de otros cursos o software no quieres tocar el Java por defecto de tu sistema operativo, puedes obligar **únicamente a tu entorno virtual** a usar Java 17 definiendo la variable de entorno `JAVA_HOME`.

1. Busca dónde se ha instalado Java 17:
```bash
ls /usr/lib/jvm/

```


(Verás una carpeta llamada algo así como `java-17-openjdk-amd64`).
2. En tu terminal, justo antes de ejecutar tu script de Python (o dentro de tu entorno virtual activado), exporta la variable:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

```



Si haces esto, cuando `pyspark` busque Java, irá directamente a esa ruta ignorando el Java 8 del sistema. Una vez soluciones esto, verás que el script local arranca del tirón en modo `local[*]`.