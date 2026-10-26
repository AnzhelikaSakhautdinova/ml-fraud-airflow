FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && apt-get clean
RUN mkdir -p /opt/project/runtime /opt/airflow/state && chown -R airflow:root /opt/project /opt/airflow/state
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir 'apache-airflow==2.10.5' -r /tmp/requirements.txt
COPY --chown=airflow:root app /opt/project/app
COPY --chown=airflow:root pipeline /opt/project/pipeline
COPY --chown=airflow:root scripts /opt/project/scripts
COPY --chown=airflow:root tests /opt/project/tests
COPY --chown=airflow:root examples /opt/project/examples
COPY --chown=airflow:root data/seed.csv /opt/project/data/seed.csv
COPY --chown=airflow:root dags /opt/airflow/dags
ENV PYTHONPATH=/opt/project OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2
WORKDIR /opt/project
