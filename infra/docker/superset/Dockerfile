FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir trino==0.329.0 sqlalchemy-trino==0.5.0
USER superset
