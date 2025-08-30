FROM python:3.13-slim

WORKDIR /app

# Instala dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código
COPY . .

# Cloud Run escucha en 8080
EXPOSE 8080

# Arranque con Gunicorn -> main:app
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app", "--workers", "2", "--timeout", "120"]
