FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 DBT_PROFILES_DIR=/workspace/dbt DBT_SEND_ANONYMOUS_USAGE_STATS=false
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dbt dbt
RUN cd dbt && dbt deps
COPY scripts scripts
COPY .sqlfluff .sqlfluff
COPY .pre-commit-config.yaml .pre-commit-config.yaml
COPY Makefile Makefile
CMD ["dbt", "--version"]
