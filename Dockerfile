FROM python:slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY . .
CMD ["python", "app.py"]
