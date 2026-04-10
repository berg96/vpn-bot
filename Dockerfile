FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir aiogram==3.14.0 aiohttp==3.10.11

COPY *.py ./

CMD ["python", "bot.py"]
