FROM python:3.12-slim

WORKDIR /app

COPY shared /app/shared
RUN pip install --no-cache-dir -e /app/shared

COPY services/message-consumer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/message-consumer/src /app/src

CMD ["python", "-m", "src.main"]
