FROM docker.io/library/python:3.15-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data \
    WEB_PORT=8081 \
    VIEW_PORT=8082

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY automaticky_provoz.py app.py ./
COPY static ./static

VOLUME /data
EXPOSE 8081 8082
CMD ["python", "app.py"]
