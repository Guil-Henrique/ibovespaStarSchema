# Usar uma imagem Python
FROM python:3.9-slim

# Instalar dependências do sistema necessárias para compilar o psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar os arquivos de requisitos para o contêiner
COPY requirements.txt .

# Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código da aplicação para o diretório de trabalho
COPY . .

# Executar o script principal
CMD ["python", "main.py"]
