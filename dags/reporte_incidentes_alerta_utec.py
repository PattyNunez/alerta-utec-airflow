from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import json
import os
from collections import Counter

import boto3
from botocore.exceptions import ClientError
import requests  


# Ruta del repo (carpeta padre de /dags)
BASE_PATH = os.path.dirname(os.path.dirname(__file__))
DATA_PATH = os.path.join(BASE_PATH, "data")

INPUT_FILE = os.path.join(DATA_PATH, "incidentes_mock.json")
OUTPUT_FILE = os.path.join(DATA_PATH, "reporte_incidentes.json")

# Nombre del bucket S3 (cambiar nombre x la variable de entorno)
S3_BUCKET = "alerta-utec-reportes" 


# funciones del DAG


def extraer_incidentes(**context):
    url = "https://zisd0y9a8j.execute-api.us-east-1.amazonaws.com/dev"

    incidentes = None

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        incidentes = response.json()
        print("Usando datos del API real.")
    except Exception as e:
        print("Error al obtener datos del API, usando mock:", e)

    if incidentes is None:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            incidentes = json.load(f)
        print("Incidentes cargados desde incidentes_mock.json")
        
    context["ti"].xcom_push(key="incidentes", value=incidentes)




def calcular_estadisticas(**context):
    """
    Calcula estadísticas: totales, por estado, por tipo y por urgencia.
    Guarda el reporte localmente y también lo envía por XCom.
    """
    incidentes = context["ti"].xcom_pull(key="incidentes") or []

    total = len(incidentes)
    estados = Counter(i.get("estado", "desconocido") for i in incidentes)
    tipos = Counter(i.get("tipo", "desconocido") for i in incidentes)
    urgencias = Counter(i.get("urgencia", "desconocida") for i in incidentes)

    reporte = {
        "fecha_generacion": datetime.utcnow().isoformat(),
        "totales": {
            "incidentes": total
        },
        "por_estado": dict(estados),
        "por_tipo": dict(tipos),
        "por_urgencia": dict(urgencias),
    }

    # Guardar reporte local (para pruebas)
    os.makedirs(DATA_PATH, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2)

    # Enviar a siguiente tarea
    context["ti"].xcom_push(key="reporte", value=reporte)


def subir_a_s3(**context):
    """
    Sube el reporte a S3 si el bucket está configurado.
    Si no, solo imprime el reporte como fallback.
    """
    reporte = context["ti"].xcom_pull(key="reporte") or {}

    if not S3_BUCKET:
        print("[AVISO] No se configuró 'S3_REPORTS_BUCKET'.")
        print("Mostrando reporte generado:")
        print(json.dumps(reporte, ensure_ascii=False, indent=2))
        return

    s3 = boto3.client("s3")

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    key = f"incidentes/{datetime.utcnow().strftime('%Y/%m/%d')}/reporte_{timestamp}.json"

    body = json.dumps(reporte, ensure_ascii=False, indent=2).encode("utf-8")

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body)
        print(f"Reporte subido correctamente a s3://{S3_BUCKET}/{key}")
    except ClientError as e:
        print(f" Error al subir a S3: {e}")


# Definición del DAG


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="reporte_incidentes_alerta_utec",
    description="DAG para generar reportes de incidentes de Alerta UTEC",
    start_date=datetime(2025, 11, 15),
    schedule_interval="0 * * * *",  # cada hora
    catchup=False,
    default_args=default_args,
    tags=["alerta-utec", "reportes"],
) as dag:

    extraer = PythonOperator(
        task_id="extraer_incidentes",
        python_callable=extraer_incidentes,
    )

    calcular = PythonOperator(
        task_id="calcular_estadisticas",
        python_callable=calcular_estadisticas,
    )

    subir = PythonOperator(
        task_id="subir_a_s3",
        python_callable=subir_a_s3,
    )

    extraer >> calcular >> subir
