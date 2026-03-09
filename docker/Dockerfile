FROM python:3.11-slim

LABEL maintainer="Gabriel Demetrios Lafis <gabriel.lafis@gmail.com>"
LABEL description="Flink Real-Time Feature Computation Engine"

WORKDIR /app

# System dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY src/ ./src/
COPY config/ ./config/
COPY main.py .

# Non-root user
RUN useradd --create-home appuser
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import src; print('healthy')" || exit 1

ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV LOG_FORMAT=json

ENTRYPOINT ["python"]
CMD ["main.py"]
