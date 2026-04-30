FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proxy.py scoutline.html manage_users.py train_model.py ./

EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8081/health')" || exit 1

CMD ["python3", "proxy.py"]
