FROM mcr.microsoft.com/playwright/python:v1.56.0-noble

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Browser ist im Playwright-Image grundsätzlich vorbereitet; dieser Befehl stellt sicher,
# dass die Python-Playwright-Version ihren Chromium-Browser findet.
RUN python -m playwright install chromium

COPY . .

CMD ["bash", "-lc", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
