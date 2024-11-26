FROM python:3.12.7-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y kafkacat

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY cap.py ./

CMD ["python", "cap.py"]
