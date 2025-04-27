FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .
ENV PORT=8080
CMD ["gunicorn", "--bind", ":8080", "main:app"]
