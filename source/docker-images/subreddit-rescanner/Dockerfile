FROM python:3.8-slim
WORKDIR /app
COPY ./docker-images/subreddit-rescanner/src .
COPY ./docker-images/subreddit-rescanner/requirements.txt .
COPY ./talos ./talos
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "./main.py"]