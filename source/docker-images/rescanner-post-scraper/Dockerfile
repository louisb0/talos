FROM python:3.8-slim
WORKDIR /app
COPY ./docker-images/rescanner-post-scraper/src .
COPY ./docker-images/rescanner-post-scraper/requirements.txt .
COPY ./talos ./talos
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "./main.py"]