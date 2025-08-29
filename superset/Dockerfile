FROM apache/superset:latest

USER root

# Install dependencies for mysqlclient
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    pkg-config \
    libmariadb-dev \
    libmariadb-dev-compat \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir mysqlclient pinotdb

# Set environment variables
ENV ADMIN_USERNAME=$ADMIN_USERNAME
ENV ADMIN_EMAIL=$ADMIN_EMAIL
ENV ADMIN_PASSWORD=$ADMIN_PASSWORD

# Copy initialization script and config file
COPY ./superset-init.sh /superset-init.sh
COPY superset_config.py /app/

# Set Superset config path
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset
ENTRYPOINT ["/superset-init.sh"]
