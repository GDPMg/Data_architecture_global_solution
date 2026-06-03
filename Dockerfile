FROM apache/airflow:2.8.4-python3.11

# Instala dependências extras além do Airflow base
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir oracledb requests
