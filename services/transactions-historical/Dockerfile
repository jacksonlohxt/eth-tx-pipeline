FROM python:3.12-slim

WORKDIR /app

COPY shared /app/shared
RUN pip install --no-cache-dir -e /app/shared

COPY services/transactions-historical/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/transactions-historical/src /app/src
COPY services/transactions-historical/tests/fixtures /app/tests/fixtures

CMD ["python", "-m", "src.main"]
