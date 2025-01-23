FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir Flask pandas pyarrow

EXPOSE 6007

CMD ["python", "app.py"]