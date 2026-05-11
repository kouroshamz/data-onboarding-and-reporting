FROM python:3.11-slim

WORKDIR /app

# System dependencies for WeasyPrint PDF export + PostgreSQL client
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    libpq-dev \
    gcc \
    libcairo2 \
    libffi-dev \
    libgdk-pixbuf-2.0-0 \
    libharfbuzz-subset0 \
    libjpeg62-turbo \
    libopenjp2-7 \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir weasyprint

# Copy application code
COPY app/ ./app/
COPY config.example.yaml ./

# Create output directories
RUN mkdir -p /app/reports /app/logs

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "app.cli"]
CMD ["run", "--config", "config.yaml"]
