FROM python:3.9-slim-buster
WORKDIR /app


COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Команда за замовчуванням для запуску контейнера
# Це буде точка входу для міграції даних
# Ви можете змінити її на `CMD ["python", "run_queries.py"]` для запуску аналітики
CMD ["python", "migrate_data.py"]