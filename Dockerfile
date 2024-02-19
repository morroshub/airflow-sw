FROM apache/airflow:2.8.1

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

