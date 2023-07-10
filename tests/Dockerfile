FROM python:3.8-slim
WORKDIR /app
COPY ./tests/requirements.txt .
COPY ./tests/tests ./tests
COPY ./source/docker-images ./docker-images
COPY ./source/talos ./talos
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "-m", "unittest", "discover", "-v", "tests"]