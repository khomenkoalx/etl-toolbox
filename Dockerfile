# Dockerfile
FROM python:3.12-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . .

# Создаём структуру data по умолчанию (на случай, если том не примонтирован)
RUN mkdir -p ./data/input ./data/validated ./data/processed ./data/errors
