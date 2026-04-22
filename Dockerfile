# Streamlit + same Python env as the ETL (nyc_taxi). Default CMD runs the app.
# Build: docker build -t nyc-taxi .
# App:   docker run -p 8501:8501 nyc-taxi
# ETL:   docker run --rm nyc-taxi python -m nyc_taxi -q
# Mount a volume for persistence:  -v $(pwd)/data:/opt/app/data  -v $(pwd)/output:/opt/app/output
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MPLBACKEND=Agg

WORKDIR /opt/app

# ca-certificates helps HTTPS to TLC; slim image is otherwise sufficient for prebuilt wheels
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY nyc_taxi/ ./nyc_taxi/
COPY app.py ./

EXPOSE 8501

# Streamlit exposes /_stcore/health on recent versions; extend start-period for first ETL if you change CMD
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=5)" || exit 1

# Bind all interfaces for AWS / OCI / Docker host port mapping
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
