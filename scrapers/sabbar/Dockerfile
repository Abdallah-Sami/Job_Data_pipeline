# Sabbar needs a real browser (Playwright + Chromium), so it uses the
# official Playwright image. Keep the playwright version in requirements.txt
# identical to the image tag.
FROM mcr.microsoft.com/playwright/python:v1.63.0-noble

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sabbar_scraper_cloud.py .

CMD ["python", "sabbar_scraper_cloud.py"]
