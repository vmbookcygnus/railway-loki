FROM python:3.11-slim
RUN pip install boto3 requests
COPY ship.py /app/ship.py
CMD ["python3", "/app/ship.py"]