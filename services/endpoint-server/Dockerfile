FROM python:3.12-slim

WORKDIR /app

COPY shared /app/shared
RUN pip install --no-cache-dir -e /app/shared

COPY services/endpoint-server/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/endpoint-server/src /app/src

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
