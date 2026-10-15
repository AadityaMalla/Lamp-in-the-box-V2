# =============================================================
# Author : Aaditya Malla, Khadija Osman, Natoli Mayu
# File   : spark/Dockerfile
# Purpose: Spark SQL + Delta Lake + S3 (s3a) service image
# =============================================================
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "pyspark==4.0.*" "delta-spark==4.0.*"
WORKDIR /app
COPY server.py warmup.py ./
RUN python warmup.py
EXPOSE 9000 4040
CMD ["python", "-u", "server.py"]
