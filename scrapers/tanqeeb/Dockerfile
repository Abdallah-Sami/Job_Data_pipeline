# Lightweight image: plain requests + BeautifulSoup, no browser needed.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tanqeeb_scraper_cloud.py .

CMD ["python", "tanqeeb_scraper_cloud.py"]
