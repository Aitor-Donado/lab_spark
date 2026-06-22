#!/usr/bin/env python3
import io
import json
import os
import boto3
from PIL import Image
from pyspark.sql import SparkSession

# ==========================================
# 1. CONFIGURACIÓN CENTRALIZADA
# ==========================================
IP_CENTRAL = "192.168.8.41"
BUCKET_NAME = "bacteria-dataset"
PREFIX_DATASET = "AGAR_demo_extracted/AGAR_representative/"

TARGET_WIDTH, TARGET_HEIGHT = 1024, 1024
ORIGINAL_WIDTH, ORIGINAL_HEIGHT = 2048, 2048
SCALE_X = TARGET_WIDTH / ORIGINAL_WIDTH
SCALE_Y = TARGET_HEIGHT / ORIGINAL_HEIGHT


# ==========================================
# 2. CLIENTE DE ALMACENAMIENTO (BOTO3)
# ==========================================
def get_s3_client():
    """Devuelve un cliente de boto3 conectado al MinIO central."""
    return boto3.client(
        's3',
        endpoint_url=f"http://{IP_CENTRAL}:9000",
        aws_access_key_id="admin_clase",
        aws_secret_access_key="PasswordClase2026",
        config=boto3.session.Config(signature_version='s3v4')
    )


# ==========================================
# 3. MÓDULOS DE ENTRADA Y SALIDA (I/O)
# ==========================================
def s3_read_json(s3_client, bucket, s3_key):
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    return json.loads(response['Body'].read().decode('utf-8'))

def s3_read_image(s3_client, bucket, s3_key):
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    return Image.open(io.BytesIO(response['Body'].read()))

def s3_write_json(s3_client, bucket, s3_key, data):
    json_buffer = io.StringIO()
    json.dump(data, json_buffer, indent=4)
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=json_buffer.getvalue())

def s3_write_image(s3_client, bucket, s3_key, pil_image):
    img_buffer = io.BytesIO()
    pil_image.save(img_buffer, format="JPEG")
    img_buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=img_buffer)


# ==========================================
# 4. MÓDULOS DE TRANSFORMACIÓN (LÓGICA)
# ==========================================
def resize_image_and_labels(img, original_labels):
    """Reescala la imagen y recalcula las cajas del JSON de forma proporcional."""
    img_resized = img.resize((TARGET_WIDTH, TARGET_HEIGHT))
    new_labels = []
    for label in original_labels:
        new_labels.append({
            "id": label["id"],
            "class": label["class"],
            "x": int(label["x"] * SCALE_X),
            "y": int(label["y"] * SCALE_Y),
            "width": int(label["width"] * SCALE_X),
            "height": int(label["height"] * SCALE_Y)
        })
    return img_resized, new_labels

def crop_bacterial_colony(img, label):
    """Realiza un recorte (crop) de una colonia bacteriana."""
    x, y, w, h = label["x"], label["y"], label["width"], label["height"]
    return img.crop((x, y, x + w, y + h))


# ==========================================
# 5. ORQUESTADOR DISTRIBUIDO (SPARK WORKER)
# ==========================================
def process_image_and_json(json_key):
    """Función que se ejecutará en paralelo en los nodos de los alumnos."""
    filename_base = os.path.basename(json_key).replace(".json", "")
    img_key = json_key.replace(".json", ".jpg")
    
    s3 = get_s3_client()
    
    try:
        # FASE 1: Lectura desde MinIO en memoria
        data = s3_read_json(s3, BUCKET_NAME, json_key)
        img = s3_read_image(s3, BUCKET_NAME, img_key)
        
        # FASE 2: Transformación (Reducción de resolución)
        img_resized, new_labels = resize_image_and_labels(img, data.get("labels", []))
        
        # FASE 3: Generación de Crops distribuidos
        for label in data.get("labels", []):
            colony_crop = crop_bacterial_colony(img, label)
            crop_s3_key = f"output/crops/{label['class']}/{data['sample_id']}_{label['id']}.jpg"
            s3_write_image(s3, BUCKET_NAME, crop_s3_key, colony_crop)
            
        # FASE 4: Volcado de resultados finales procesados
        s3_write_image(s3, BUCKET_NAME, f"output/segmentation/images/{filename_base}.jpg", img_resized)
        
        data["labels"] = new_labels
        s3_write_json(s3, BUCKET_NAME, f"output/segmentation/labels/{filename_base}.json", data)
        
        return f"OK: {filename_base}"
    except Exception as e:
        return f"ERROR en {filename_base}: {str(e)}"


# ==========================================
# 6. ENTRADA PRINCIPAL (SPARK MASTER)
# ==========================================
if __name__ == "__main__":
    print("[INFO] Listando ficheros JSON en MinIO usando el Driver...")
    
    # El Driver lista usando Python puro (Nos saltamos la configuración de Hadoop Java)
    s3_driver = get_s3_client()
    response = s3_driver.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX_DATASET)
    
    json_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]
    total_archivos = len(json_keys)
    
    if total_archivos == 0:
        print("[ERROR] No se han encontrado archivos JSON en el bucket. Revisa la ruta.")
        exit(1)
        
    print(f"[INFO] Detectados {total_archivos} archivos JSON. Inicializando Spark...")

    # Inicialización limpia de la SparkSession nativa
    spark = SparkSession.builder \
        .master(f"spark://{IP_CENTRAL}:7077") \
        .config("spark.driver.host", IP_CENTRAL) \
        .appName("BacteriaDataset_Boto3_Pipeline") \
        .getOrCreate()

    print("[INFO] Distribuyendo tareas en el clúster...")
    paths_rdd = spark.sparkContext.parallelize(json_keys)

    # Lanzamos el procesamiento distribuido real
    results = paths_rdd.map(process_image_and_json).collect()

    # --- MÉTRICAS Y FEEDBACK DE CONTROL ---
    exitos = sum(1 for res in results if "OK:" in res)
    errores = sum(1 for res in results if "ERROR" in res)

    print("\n=============================================")
    print("      RESUMEN DEL PROCESAMIENTO CLUSTER     ")
    print("=============================================")
    print(f"Total archivos planificados : {total_archivos}")
    print(f"✅ Tareas finalizadas con éxito: {exitos}")
    print(f"❌ Tareas fallidas en workers  : {errores}\n")

    if errores > 0:
        print("Muestra de errores encontrados:")
        for err in [r for r in results if "ERROR" in r][:5]:
            print(f"  > {err}")

    # Cerrar la sesión del clúster de forma controlada
    spark.stop()
    print("[INFO] Proceso finalizado. Sesión de Spark cerrada.")