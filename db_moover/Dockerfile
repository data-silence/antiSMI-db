FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY app .

LABEL authors="data-silence"
LABEL app_name='moover'

RUN apt-get -y update && apt-get install -y mc curl

ENTRYPOINT ["python3", "main.py"]
