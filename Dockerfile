FROM python:3.11-slim

# Evita bytecode y buffer (mejor logs)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Instala deps del sistema usados comúnmente por pip (y limpia cache)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Instala dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código
COPY . .

# Cloud Run escucha en 8080
ENV PORT=8080

# Arranque del contenedor: gunicorn en 0.0.0.0:8080 y main:app
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app", "--workers", "2", "--timeout", "120"]
