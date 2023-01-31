FROM python:3.8.16-slim

WORKDIR /app

COPY generator.py generator.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

CMD [ "python", "-u", "generator.py","--project_id","e2e-serverless-edem","--topic_name","sensor_info" ]