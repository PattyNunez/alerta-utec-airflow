from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import json
from collections import Counter

import boto3
from botocore.exceptions import ClientError
import requests


# Configuración
API_URL = "https://zisd0y9a8j.execute-api.us-east-1.amazonaws.com/dev"
S3_BUCKET = "alerta-utec-reportes"
S3_MOCK_KEY = "data/incidentes_mock.json"


def extraer_incidentes(**context):
    """
    Extrae incidentes del API. Si falla, usa el mock desde S3.
    """
    incidentes = None
    
    # Intentar obtener desde el API
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        incidentes = response.json()
        print(f"✓ Datos obtenidos del API: {len(incidentes)} incidentes")
    except Exception as e:
        print(f"⚠ Error al obtener datos del API: {e}")
        print("Intentando cargar datos mock desde S3...")
        
        # Fallback: cargar mock desde S3
        try:
            s3 = boto3.client("s3")
            response = s3.get_object(Bucket=S3_BUCKET, Key=S3_MOCK_KEY)
            incidentes = json.loads(response['Body'].read().decode('utf-8'))
            print(f"✓ Datos mock cargados desde S3: {len(incidentes)} incidentes")
        except Exception as s3_error:
            print(f"⚠ Error al cargar mock desde S3: {s3_error}")
            incidentes = []
    
    if not incidentes:
        print("⚠ No se pudieron obtener incidentes de ninguna fuente")
        incidentes = []
    
    context["ti"].xcom_push(key="incidentes", value=incidentes)


def calcular_estadisticas(**context):
    """
    Calcula estadísticas: totales, por estado, por tipo y por urgencia.
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

    print(f"✓ Estadísticas calculadas: {total} incidentes procesados")
    context["ti"].xcom_push(key="reporte", value=reporte)


def subir_a_s3(**context):
    """
    Sube el reporte a S3.
    """
    reporte = context["ti"].xcom_pull(key="reporte") or {}

    if not S3_BUCKET:
        print("⚠ No se configuró S3_BUCKET")
        print(json.dumps(reporte, ensure_ascii=False, indent=2))
        return

    s3 = boto3.client("s3")
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    key = f"reportes/{datetime.utcnow().strftime('%Y/%m/%d')}/reporte_{timestamp}.json"

    body = json.dumps(reporte, ensure_ascii=False, indent=2).encode("utf-8")

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body)
        print(f"✓ Reporte subido a s3://{S3_BUCKET}/{key}")
    except ClientError as e:
        print(f"✗ Error al subir a S3: {e}")
        raise


# Definición del DAG

default_args = {
    "owner": "alerta-utec",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
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
