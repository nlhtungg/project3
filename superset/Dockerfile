FROM apache/superset:5.0.0

USER root

# Install Trino driver directly into the venv site-packages
RUN /usr/local/bin/pip install --target=/app/.venv/lib/python3.10/site-packages sqlalchemy-trino

USER superset
