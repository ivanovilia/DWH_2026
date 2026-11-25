FROM python:3.11-slim
RUN pip install --no-cache-dir boto3==1.36.3 confluent-kafka==2.15.0
WORKDIR /app
