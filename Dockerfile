FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY config/*.py config/
COPY src/ src/
COPY scripts/ scripts/
COPY tests/ tests/
COPY sql/ sql/
COPY main.py ./
RUN python -m scripts.download_jdbc
ENV PYTHONUNBUFFERED=1 PYSPARK_PYTHON=python
CMD ["python", "main.py"]
