# Usa imagem leve do Python
FROM python:3.9-slim

# Define diretório de trabalho
WORKDIR /app

# Instala dependências do sistema necessárias (opcional, mas bom pra garantir)
RUN apt-get update && apt-get install -y gcc

# Copia e instala requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da API
COPY normalizador_api.py .

# Expõe a porta 8000
EXPOSE 8000

# Comando para iniciar a API
CMD ["uvicorn", "normalizador_api:app", "--host", "0.0.0.0", "--port", "8000"]
