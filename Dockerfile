# 1. Escolhe a imagem base (Python)
FROM python:3.11-slim

# 2. Define o diretório de trabalho dentro do container
WORKDIR /app

# 3. Define a variável de ambiente PORTA que o Gunicorn vai usar
ENV PORT 8000

# 4. Instala as dependências de produção (Gunicorn, Flask, Pandas, etc.)
# Copia o arquivo de requisitos
COPY requirements.txt .
# Instala as bibliotecas
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia o código da sua API para o container
# Assume que seu código Flask está em 'normalizador_api.py'
COPY . .

# 6. Comando para iniciar o servidor Gunicorn
# Ele executa o Gunicorn e o vincula ao host 0.0.0.0 e à porta 8000
CMD ["gunicorn", "normalizador_api:app", "--bind", "0.0.0.0:8000"]
