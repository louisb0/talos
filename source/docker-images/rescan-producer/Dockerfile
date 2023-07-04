FROM python:3.8-slim
WORKDIR /app
COPY ./docker-images/rescan-producer/src .
COPY ./docker-images/rescan-producer/requirements.txt .
COPY ./talos .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "./main.py"]
