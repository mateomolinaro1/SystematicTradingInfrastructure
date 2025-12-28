FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "-u"]


