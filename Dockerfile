# --- Base ---
FROM python:3.10-slim

# Evita salida buffer
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo
WORKDIR /app

# Copia requirements
COPY requirements.txt /app/

# Instala dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia código fuente
COPY . /app/

# Puerto estándar FastAPI
EXPOSE 8080

# Comando de arranque
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
