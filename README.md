# alerta-utec-airflow
Descripción

Este proyecto implementa un flujo que extrae incidentes desde un API REST, calcula estadísticas, genera un reporte en formato JSON y lo guarda en Amazon S3. El proceso se ejecuta cada hora para mantener la información siempre actualizada.



Componentes del sistema:

API Gateway como fuente de datos.

Airflow encargado de orquestar el ETL.

Amazon S3 para almacenar los reportes procesados.

ECS Fargate como infraestructura para correr Airflow en contenedores.

Estructura del Proyecto
alerta-utec-airflow/
├── Dockerfile
├── requirements.txt
├── dags/
│   └── reporte_incidentes_alerta_utec.py
└── data/
    └── incidentes_mock.json

Pipeline del DAG

Flujo general:

extraer_incidentes → calcular_estadisticas → subir_a_s3

Tareas principales

extraer_incidentes
Consulta el API de incidentes. Si el servicio no responde, utiliza un dataset mock. También se encarga de limpiar y validar los datos.

calcular_estadisticas
Genera totales y agrupaciones por estado, tipo y urgencia. Añade un timestamp de generación al reporte.

subir_a_s3
Guarda el archivo JSON en S3 utilizando una estructura basada en fecha. Maneja errores de conexión o permisos.

Configuración del DAG

Frecuencia: cada hora

Reintentos: 2 (espera de 5 minutos entre intentos)

Catch-up deshabilitado

Tecnologías Utilizadas

Python 3.11+

Apache Airflow 2.8.1

Docker y Docker Compose

Amazon S3, ECS Fargate, ECR y API Gateway

Dependencias
boto3
requests

Instalación y Deploy
Docker local
git clone https://github.com/PattyNunez/alerta-utec-airflow.git
cd alerta-utec-airflow

docker build -t airflow-alerta-utec .

docker run -p 8080:8080 \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  airflow-alerta-utec


Airflow estará disponible en: http://localhost:8080

AWS ECS Fargate
export AWS_ACCOUNT_ID=your_account_id
export AWS_REGION=us-east-1
export REPO_NAME=airflow-alerta-utec

aws ecr create-repository --repository-name ${REPO_NAME}

docker build -t ${REPO_NAME} .
docker tag ${REPO_NAME}:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}:latest
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}:latest

Configuración

Variables de entorno:

API_URL=https://your-api-gateway.amazonaws.com/dev
S3_REPORTS_BUCKET=alerta-utec-reportes
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags


Permisos mínimos recomendados:

s3:GetObject y s3:PutObject

Permisos de logs para CloudWatch

Formato del Reporte
{
  "fecha_generacion": "2025-11-17T04:30:00.000000",
  "totales": { "incidentes": 15 },
  "por_estado": { "pendiente": 5, "en_atencion": 7, "resuelto": 3 },
  "por_tipo": { "servicios": 6, "infraestructura": 4, "seguridad": 5 },
  "por_urgencia": { "alta": 3, "media": 8, "baja": 4 }
}

Monitoreo

Logs en CloudWatch

Métricas desde la interfaz de Airflow

Alarmas para fallos del DAG

Problemas Comunes

El DAG no aparece en la UI
Verifica la sintaxis del archivo y revisa los logs del scheduler.

Problemas con S3
Confirmar credenciales, permisos y nombre del bucket.

Falla el API
El pipeline usará datos mock automáticamente.

Equipo

Desarrollo: Alerta UTEC Team

Curso: Cloud Computing (CS2032), UTEC

Licencia

Proyecto desarrollado para el Hackathon CS2032 – UTEC 2025.

Enlaces

Repositorio: https://github.com/PattyNunez/alerta-utec-airflow

Documentación de Airflow

Documentación de AWS Fargate
