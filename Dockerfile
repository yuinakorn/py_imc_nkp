FROM python:3.10.1-slim

WORKDIR /app

COPY requirements.txt .

# RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
