FROM python:3.11-slim

WORKDIR /app

COPY proxy.py scoutline.html manage_users.py ./

EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8081/health')" || exit 1

CMD ["python3", "proxy.py"]
