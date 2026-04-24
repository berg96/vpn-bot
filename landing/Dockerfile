FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi==0.115.0 \
    uvicorn[standard]==0.32.0 \
    jinja2==3.1.4 \
    aiohttp==3.10.11 \
    python-multipart==0.0.12 \
    aiogram==3.14.0

COPY db.py marzban.py tinkoff.py config.py ./
COPY landing/ ./landing/

EXPOSE 8081

CMD ["uvicorn", "landing.app:app", "--host", "0.0.0.0", "--port", "8081", "--forwarded-allow-ips=*"]
